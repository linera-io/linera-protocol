// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use allocative::Allocative;
use linera_base::{
    data_types::{ArithmeticError, BlockHeight, Cursor},
    ensure,
    identifiers::ChainId,
};
#[cfg(with_testing)]
use linera_views::context::MemoryContext;
use linera_views::{
    context::Context,
    queue_view::QueueView,
    register_view::RegisterView,
    views::{ClonableView, View},
    ViewError,
};
use thiserror::Error;

use crate::{data_types::MessageBundle, ChainError};

#[cfg(test)]
#[path = "unit_tests/inbox_tests.rs"]
mod inbox_tests;

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_interval, register_histogram_vec};
    use prometheus::HistogramVec;

    pub static INBOX_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "inbox_size",
            "Inbox size",
            &[],
            exponential_bucket_interval(1.0, 2_000_000.0),
        )
    });

    pub static REMOVED_BUNDLES: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "removed_bundles",
            "Number of bundles removed by anticipation",
            &[],
            exponential_bucket_interval(1.0, 10_000.0),
        )
    });
}

/// The state of an inbox.
/// * An inbox is used to track bundles received and executed locally.
/// * A `MessageBundle` consists of a logical cursor `(height, index)` and some message
///   content `messages`.
/// * On the surface, an inbox looks like a FIFO queue: the main APIs are `add_bundle` and
///   `remove_bundle`.
/// * However, bundles can also be removed before they are added. When this happens,
///   the bundles removed by anticipation are tracked in a separate queue. Any bundle added
///   later will be required to match the first removed bundle and so on.
/// * The cursors of added bundles (resp. removed bundles) must be increasing over time.
/// * Reconciliation of added and removed bundles is allowed to skip some added bundles.
///   However, the opposite is not true: every removed bundle must be eventually added.
#[cfg_attr(with_graphql, derive(async_graphql::SimpleObject))]
#[derive(Allocative, Debug, ClonableView, View)]
#[allocative(bound = "C")]
pub struct InboxStateView<C>
where
    C: Clone + Context,
{
    /// We have already added all the messages below this height and index.
    pub next_cursor_to_add: RegisterView<C, Cursor>,
    /// We have already removed all the messages below this height and index.
    pub next_cursor_to_remove: RegisterView<C, Cursor>,
    /// These bundles have been added and are waiting to be removed.
    pub added_bundles: QueueView<C, MessageBundle>,
    /// These bundles have been removed by anticipation and are waiting to be added.
    /// At least one of `added_bundles` and `removed_bundles` should be empty.
    pub removed_bundles: QueueView<C, MessageBundle>,
    /// When this inbox was restored from a checkpoint, the sender's
    /// `next_cursor_to_remove` at that time. Bundles with cursors strictly below this
    /// are silently dropped from `add_bundle` / `remove_bundle`: their effects are
    /// already baked into the restored execution state, so re-delivering them on this
    /// validator (e.g. when a sender re-pushes) must be a no-op rather than re-enter
    /// the inbox or the removed-by-anticipation queue.
    pub restored_cursor: RegisterView<C, Cursor>,
    /// The lowest cursor at which the sender may still push a new bundle. Set when an
    /// update's first bundle declares no predecessor: the sender's checkpoint settled
    /// the lane below that bundle, so nothing below is re-pushable — a node that never
    /// received those bundles can only learn of their consumption through this chain's
    /// certified blocks. Below this cursor, consumptions that cannot be reconciled with
    /// `added_bundles` are ignored instead of raising `UnexpectedBundle`, nothing is
    /// anticipated, and re-deliveries are dropped. Bundles actually present in
    /// `added_bundles` are unaffected: they still reconcile (or get skipped) normally.
    pub sender_pruned_cursor: RegisterView<C, Cursor>,
}

#[derive(Error, Debug)]
pub(crate) enum InboxError {
    #[error(transparent)]
    ViewError(#[from] ViewError),
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error("Cannot reconcile {bundle:?} with {previous_bundle:?}")]
    UnexpectedBundle {
        bundle: MessageBundle,
        previous_bundle: MessageBundle,
    },
    #[error("{bundle:?} is out of order. Block and height should be at least: {next_cursor:?}")]
    IncorrectOrder {
        bundle: MessageBundle,
        next_cursor: Cursor,
    },
    #[error(
        "{bundle:?} cannot be skipped: it must be received before the next \
        messages from the same origin"
    )]
    UnskippableBundle { bundle: MessageBundle },
}

impl From<(ChainId, ChainId, InboxError)> for ChainError {
    fn from(value: (ChainId, ChainId, InboxError)) -> Self {
        let (chain_id, origin, error) = value;
        match error {
            InboxError::ViewError(e) => ChainError::ViewError(e),
            InboxError::ArithmeticError(e) => ChainError::ArithmeticError(e),
            InboxError::UnexpectedBundle {
                bundle,
                previous_bundle,
            } => ChainError::UnexpectedMessage {
                chain_id,
                origin,
                bundle: Box::new(bundle),
                previous_bundle: Box::new(previous_bundle),
            },
            InboxError::IncorrectOrder {
                bundle,
                next_cursor,
            } => ChainError::IncorrectMessageOrder {
                chain_id,
                origin,
                bundle: Box::new(bundle),
                next_height: next_cursor.height,
                next_index: next_cursor.index,
            },
            InboxError::UnskippableBundle { bundle } => ChainError::CannotSkipMessage {
                chain_id,
                origin,
                bundle: Box::new(bundle),
            },
        }
    }
}

impl<C> InboxStateView<C>
where
    C: Context + Clone + 'static,
{
    /// Converts the internal cursor for added bundles into an externally-visible block height.
    /// This makes sense because the rest of the system always adds bundles one block at a time.
    pub fn next_block_height_to_receive(&self) -> Result<BlockHeight, ChainError> {
        let cursor = self.next_cursor_to_add.get();
        if cursor.index == 0 {
            Ok(cursor.height)
        } else {
            Ok(cursor.height.try_add_one()?)
        }
    }

    /// Observes the current inbox size in the metrics histogram.
    pub fn observe_size_metric(&self) {
        #[cfg(with_metrics)]
        metrics::INBOX_SIZE
            .with_label_values(&[])
            .observe(self.added_bundles.count() as f64);
    }

    /// Reconciles this inbox with the producer's snapshot at checkpoint time.
    ///
    /// `next_cursor_to_remove` is chain-state-derived and takes the snapshot's
    /// value (a lagging validator's higher pre-restore advancement is being
    /// rolled back along with the execution state). `next_cursor_to_add` only
    /// ratchets up — a lagging validator may already have received bundles past
    /// the snapshot, and forgetting those deliveries would either lose them or
    /// trip the inbox-gap check on the next cross-chain update. Pre-existing
    /// `added_bundles` entries strictly below the cutoff are dropped (their
    /// effects are baked into the restored execution state) and the
    /// anticipated-remove queue is cleared since it came from pre-restore
    /// blocks the rollback has invalidated.
    ///
    /// Errors if `restored_cursor` already sits past `cursor`: that means a
    /// later checkpoint has already bootstrapped this inbox, and the chain
    /// worker's dispatch should never have routed an earlier one here.
    pub async fn restore_from_checkpoint(&mut self, cursor: Cursor) -> Result<(), ChainError> {
        ensure!(
            *self.restored_cursor.get() <= cursor,
            ChainError::InternalError(format!(
                "cannot restore inbox at cursor {cursor:?}: already bootstrapped \
                 from a later checkpoint at cursor {previous:?}",
                previous = *self.restored_cursor.get(),
            ))
        );
        self.restored_cursor.set(cursor);
        if *self.next_cursor_to_add.get() < cursor {
            self.next_cursor_to_add.set(cursor);
        }
        self.next_cursor_to_remove.set(cursor);
        while let Some(front) = self.added_bundles.front().await? {
            if front.cursor() >= cursor {
                break;
            }
            self.added_bundles.delete_front();
        }
        self.removed_bundles.clear();
        Ok(())
    }

    /// Records that the sender will never push a bundle with a cursor below `cursor`
    /// again, because an update declared no predecessor for its first bundle — the
    /// sender's checkpoint settled the lane below it. Anticipated bundles below the
    /// cursor are dropped: the push each of them is waiting for will never arrive, and
    /// leaving them in place would fail the reconciliation of every future push.
    pub async fn note_sender_pruned_below(&mut self, cursor: Cursor) -> Result<(), ViewError> {
        if cursor <= *self.sender_pruned_cursor.get() {
            return Ok(());
        }
        self.sender_pruned_cursor.set(cursor);
        while let Some(front) = self.removed_bundles.front().await? {
            if front.cursor() >= cursor {
                break;
            }
            self.removed_bundles.delete_front();
        }
        Ok(())
    }

    /// Consumes a bundle from the inbox.
    ///
    /// Returns `true` if the bundle was already known, i.e. it was present in `added_bundles`.
    pub(crate) async fn remove_bundle(
        &mut self,
        bundle: &MessageBundle,
    ) -> Result<bool, InboxError> {
        // Record the latest cursor.
        let cursor = bundle.cursor();
        if cursor < *self.restored_cursor.get() {
            // Bundle's effects are already in the restored execution state; treat the
            // consumption as a no-op without touching `removed_bundles` so the queue
            // doesn't fill with bundles a sender will never push again.
            return Ok(true);
        }
        ensure!(
            cursor >= *self.next_cursor_to_remove.get(),
            InboxError::IncorrectOrder {
                bundle: bundle.clone(),
                next_cursor: *self.next_cursor_to_remove.get(),
            }
        );
        // Discard added bundles with lower cursors (if any).
        while let Some(previous_bundle) = self.added_bundles.front().await? {
            if previous_bundle.cursor() >= cursor {
                break;
            }
            ensure!(
                previous_bundle.is_skippable(),
                InboxError::UnskippableBundle {
                    bundle: previous_bundle
                }
            );
            self.added_bundles.delete_front();
            tracing::trace!("Skipping previously received bundle {:?}", previous_bundle);
        }
        // Reconcile the bundle with the next added bundle, or mark it as removed.
        let already_known = match self.added_bundles.front().await? {
            Some(previous_bundle)
                if previous_bundle.cursor() > cursor
                    && cursor < *self.sender_pruned_cursor.get() =>
            {
                // The bundle was never added and never will be — but the sender's
                // checkpoint settled the lane below `sender_pruned_cursor`, so the push
                // is not missing, it is gone for good. The consumption executes from the
                // certified block's own copy of the bundle; there is nothing to
                // reconcile.
                tracing::trace!("Ignoring consumption of pruned bundle {:?}", bundle);
                false
            }
            Some(previous_bundle) => {
                // Rationale: If the two cursors are equal, then the bundles should match.
                // Otherwise, at this point we know that `self.next_cursor_to_add >
                // previous_bundle.cursor() > cursor`. Notably, `bundle` will never be
                // added in the future. Therefore, we should fail instead of adding
                // it to `self.removed_bundles`.
                ensure!(
                    bundle == &previous_bundle,
                    InboxError::UnexpectedBundle {
                        previous_bundle,
                        bundle: bundle.clone(),
                    }
                );
                self.added_bundles.delete_front();
                tracing::trace!("Consuming bundle {:?}", bundle);
                true
            }
            None if cursor < *self.sender_pruned_cursor.get() => {
                // Nothing to anticipate: the sender's checkpoint settled the lane below
                // `sender_pruned_cursor`, so this bundle will never be pushed.
                tracing::trace!("Ignoring consumption of pruned bundle {:?}", bundle);
                false
            }
            None => {
                tracing::trace!("Marking bundle as expected: {:?}", bundle);
                self.removed_bundles.push_back(bundle.clone());
                #[cfg(with_metrics)]
                metrics::REMOVED_BUNDLES
                    .with_label_values(&[])
                    .observe(self.removed_bundles.count() as f64);
                false
            }
        };
        self.next_cursor_to_remove.set(cursor.try_add_one()?);
        Ok(already_known)
    }

    /// Pushes a bundle to the inbox. The verifications should not fail in production unless
    /// many validators are faulty.
    ///
    /// Returns `true` if the bundle was new, `false` if it was already in `removed_bundles`.
    pub(crate) async fn add_bundle(&mut self, bundle: MessageBundle) -> Result<bool, InboxError> {
        // Record the latest cursor.
        let cursor = bundle.cursor();
        if cursor < *self.restored_cursor.get() {
            // The sender is re-delivering a bundle whose effects are already baked
            // into our restored execution state. Silently drop it.
            return Ok(false);
        }
        if cursor < *self.sender_pruned_cursor.get() {
            // A re-delivery racing the update that declared the lane settled below
            // `sender_pruned_cursor`. Whatever this bundle's fate — consumed and
            // acknowledged, or skipped — reconciliation for it is over. Silently drop it.
            return Ok(false);
        }
        ensure!(
            cursor >= *self.next_cursor_to_add.get(),
            InboxError::IncorrectOrder {
                bundle: bundle.clone(),
                next_cursor: *self.next_cursor_to_add.get(),
            }
        );
        // Find if the bundle was removed ahead of time.
        let newly_added = match self.removed_bundles.front().await? {
            Some(previous_bundle) => {
                if previous_bundle.cursor() == cursor {
                    // We already executed this bundle by anticipation. Remove it from
                    // the queue.
                    ensure!(
                        bundle == previous_bundle,
                        InboxError::UnexpectedBundle {
                            previous_bundle,
                            bundle,
                        }
                    );
                    self.removed_bundles.delete_front();
                    #[cfg(with_metrics)]
                    metrics::REMOVED_BUNDLES
                        .with_label_values(&[])
                        .observe(self.removed_bundles.count() as f64);
                } else {
                    // The receiver has already executed a later bundle from the same
                    // sender ahead of time so we should skip this one.
                    ensure!(
                        cursor < previous_bundle.cursor() && bundle.is_skippable(),
                        InboxError::UnexpectedBundle {
                            previous_bundle,
                            bundle,
                        }
                    );
                }
                false
            }
            None => {
                // Otherwise, schedule the messages for execution.
                self.added_bundles.push_back(bundle);
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
{
    pub async fn new() -> Self {
        let context = MemoryContext::new_for_testing(());
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}

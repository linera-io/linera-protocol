// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::SimpleObject;
use linera_base::{
    data_types::{ArithmeticError, BlockHeight},
    ensure,
    identifiers::ChainId,
};
#[cfg(with_testing)]
use linera_views::memory::{create_test_memory_context, MemoryContext};
use linera_views::{
    common::Context,
    queue_view::QueueView,
    register_view::RegisterView,
    views::{ClonableView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{data_types::MessageBundle, ChainError, Origin};

#[cfg(test)]
#[path = "unit_tests/inbox_tests.rs"]
mod inbox_tests;

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
    /// These bundles have been added and are waiting to be removed.
    pub added_bundles: QueueView<C, MessageBundle>,
    /// These bundles have been removed by anticipation and are waiting to be added.
    /// At least one of `added_bundles` and `removed_bundles` should be empty.
    pub removed_bundles: QueueView<C, MessageBundle>,
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

impl From<&MessageBundle> for Cursor {
    #[inline]
    fn from(bundle: &MessageBundle) -> Self {
        Self {
            height: bundle.height,
            index: bundle.transaction_index,
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
            InboxError::UnexpectedBundle {
                bundle,
                previous_bundle,
            } => ChainError::UnexpectedMessage {
                chain_id,
                origin: origin.into(),
                bundle,
                previous_bundle,
            },
            InboxError::IncorrectOrder {
                bundle,
                next_cursor,
            } => ChainError::IncorrectMessageOrder {
                chain_id,
                origin: origin.into(),
                bundle,
                next_height: next_cursor.height,
                next_index: next_cursor.index,
            },
            InboxError::UnskippableBundle { bundle } => ChainError::CannotSkipMessage {
                chain_id,
                origin: origin.into(),
                bundle,
            },
        }
    }
}

impl<C> InboxStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
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

    /// Consumes an bundle from the inbox.
    ///
    /// Returns `true` if the bundle was already known, i.e. it was present in `added_bundles`.
    pub(crate) async fn remove_bundle(
        &mut self,
        bundle: &MessageBundle,
    ) -> Result<bool, InboxError> {
        // Record the latest cursor.
        let cursor = Cursor::from(bundle);
        ensure!(
            cursor >= *self.next_cursor_to_remove.get(),
            InboxError::IncorrectOrder {
                bundle: bundle.clone(),
                next_cursor: *self.next_cursor_to_remove.get(),
            }
        );
        // Discard added bundles with lower cursors (if any).
        while let Some(previous_bundle) = self.added_bundles.front().await? {
            if Cursor::from(&previous_bundle) >= cursor {
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
            Some(previous_bundle) => {
                // Rationale: If the two cursors are equal, then the bundles should match.
                // Otherwise, at this point we know that `self.next_cursor_to_add >
                // Cursor::from(&previous_bundle) > cursor`. Notably, `bundle` will never be
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
            None => {
                tracing::trace!("Marking bundle as expected: {:?}", bundle);
                self.removed_bundles.push_back(bundle.clone());
                false
            }
        };
        self.next_cursor_to_remove.set(cursor.try_add_one()?);
        Ok(already_known)
    }

    /// Pushes an bundle to the inbox. The verifications should not fail in production unless
    /// many validators are faulty.
    ///
    /// Returns `true` if the bundle was new, `false` if it was already in `removed_bundles`.
    pub(crate) async fn add_bundle(&mut self, bundle: MessageBundle) -> Result<bool, InboxError> {
        // Record the latest cursor.
        let cursor = Cursor::from(&bundle);
        ensure!(
            cursor >= *self.next_cursor_to_add.get(),
            InboxError::IncorrectOrder {
                bundle: bundle.clone(),
                next_cursor: *self.next_cursor_to_add.get(),
            }
        );
        // Find if the message was removed ahead of time.
        let newly_added = match self.removed_bundles.front().await? {
            Some(previous_bundle) => {
                if Cursor::from(&previous_bundle) == cursor {
                    // We already executed this message by anticipation. Remove it from
                    // the queue.
                    ensure!(
                        bundle == previous_bundle,
                        InboxError::UnexpectedBundle {
                            previous_bundle,
                            bundle,
                        }
                    );
                    self.removed_bundles.delete_front();
                } else {
                    // The receiver has already executed a later bundle from the same
                    // sender ahead of time so we should skip this one.
                    ensure!(
                        cursor < Cursor::from(&previous_bundle) && bundle.is_skippable(),
                        InboxError::UnexpectedBundle {
                            previous_bundle,
                            bundle,
                        }
                    );
                }
                false
            }
            None => {
                // Otherwise, schedule the message for execution.
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
    ViewError: From<<MemoryContext<()> as linera_views::common::Context>::Error>,
{
    pub async fn new() -> Self {
        let context = create_test_memory_context();
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}

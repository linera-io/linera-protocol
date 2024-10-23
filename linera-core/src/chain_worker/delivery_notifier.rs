// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains a helper type to notify when cross-chain messages have been
//! delivered.
//!
//! Keeping it in a separate module ensures that only the chain worker is able to call its
//! methods.

use std::{
    collections::BTreeMap,
    mem,
    sync::{Arc, Mutex},
};

use linera_base::data_types::BlockHeight;
use tokio::sync::oneshot;
use tracing::warn;

/// A set of pending listeners waiting to be notified about the delivery of messages sent
/// from specific [`BlockHeight`]s.
///
/// The notifier instance can be cheaply `clone`d and works as a shared reference.
/// However, its methods still require `&mut self` to hint that it should only changed by
/// [`ChainWorkerStateWithAttemptedChanges`](super::ChainWorkerStateWithAttemptedChanges).
#[derive(Clone, Default)]
pub struct DeliveryNotifier {
    notifiers: Arc<Mutex<BTreeMap<BlockHeight, Vec<oneshot::Sender<()>>>>>,
}

impl DeliveryNotifier {
    /// Returns `true` if there are no pending listeners.
    pub fn is_empty(&self) -> bool {
        let notifiers = self
            .notifiers
            .lock()
            .expect("Panics should never happen while holding a lock to the `notifiers`");

        notifiers.is_empty()
    }

    /// Registers a delivery `notifier` for a desired [`BlockHeight`].
    pub(super) fn register(&mut self, height: BlockHeight, notifier: oneshot::Sender<()>) {
        let mut notifiers = self
            .notifiers
            .lock()
            .expect("Panics should never happen while holding a lock to the `notifiers`");

        notifiers.entry(height).or_default().push(notifier);
    }

    /// Notifies that all messages up to `height` have been delivered.
    pub(super) fn notify(&mut self, height: BlockHeight) {
        let relevant_notifiers = {
            let mut notifiers = self
                .notifiers
                .lock()
                .expect("Panics should never happen while holding a lock to the `notifiers`");

            let pending_notifiers = height
                .try_add_one()
                .map(|first_still_undelivered_height| {
                    notifiers.split_off(&first_still_undelivered_height)
                })
                .unwrap_or_default();

            mem::replace(&mut *notifiers, pending_notifiers)
        };

        for notifier in relevant_notifiers.into_values().flatten() {
            if let Err(()) = notifier.send(()) {
                warn!("Failed to notify message delivery to caller");
            }
        }
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use dashmap::DashMap;
use linera_base::identifiers::ChainId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::trace;

use crate::worker;

// TODO(#2171): replace this with a Tokio broadcast channel

/// A `Notifier` holds references to clients waiting to receive notifications
/// from the validator.
/// Clients will be evicted if their connections are terminated.
pub struct ChannelNotifier<N> {
    inner: DashMap<ChainId, Vec<UnboundedSender<N>>>,
}

impl<N> Default for ChannelNotifier<N> {
    fn default() -> Self {
        Self {
            inner: DashMap::default(),
        }
    }
}

impl<N> ChannelNotifier<N> {
    fn add_sender(&self, chain_ids: Vec<ChainId>, sender: &UnboundedSender<N>) {
        for id in chain_ids {
            let mut senders = self.inner.entry(id).or_default();
            senders.push(sender.clone());
        }
    }

    /// Creates a subscription given a collection of ChainIds and a sender to the client.
    pub fn subscribe(&self, chain_ids: Vec<ChainId>) -> UnboundedReceiver<N> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.add_sender(chain_ids, &tx);
        rx
    }

    /// Creates a subscription given a collection of ChainIds and a sender to the client.
    /// Immediately posts a first notification as an ACK.
    pub fn subscribe_with_ack(&self, chain_ids: Vec<ChainId>, ack: N) -> UnboundedReceiver<N> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.add_sender(chain_ids, &tx);
        tx.send(ack)
            .expect("pushing to a new channel should succeed");
        rx
    }
}

impl<N> ChannelNotifier<N>
where
    N: Clone,
{
    /// Notifies all the clients waiting for a notification from a given chain.
    pub fn notify_chain(&self, chain_id: &ChainId, notification: &N) {
        let senders_is_empty = {
            let Some(mut senders) = self.inner.get_mut(chain_id) else {
                trace!("Chain {chain_id:?} has no subscribers.");
                return;
            };
            let mut dead_senders = vec![];
            let senders = senders.value_mut();

            for (index, sender) in senders.iter_mut().enumerate() {
                if sender.send(notification.clone()).is_err() {
                    dead_senders.push(index);
                }
            }

            for index in dead_senders.into_iter().rev() {
                trace!("Removed dead subscriber for chain {chain_id:?}.");
                senders.remove(index);
            }

            senders.is_empty()
        };

        if senders_is_empty {
            trace!("No more subscribers for chain {chain_id:?}. Removing entry.");
            self.inner.remove(chain_id);
        }
    }
}

pub trait Notifier: Clone + Send + 'static {
    fn notify(&self, notifications: &[worker::Notification]);
}

impl Notifier for Arc<ChannelNotifier<worker::Notification>> {
    fn notify(&self, notifications: &[worker::Notification]) {
        for notification in notifications {
            self.notify_chain(&notification.chain_id, notification);
        }
    }
}

impl Notifier for () {
    fn notify(&self, _notifications: &[worker::Notification]) {}
}

#[cfg(with_testing)]
impl Notifier for Arc<std::sync::Mutex<Vec<worker::Notification>>> {
    fn notify(&self, notifications: &[worker::Notification]) {
        let mut guard = self.lock().unwrap();
        guard.extend(notifications.iter().cloned())
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        sync::{atomic::Ordering, Arc},
        time::Duration,
    };

    use super::*;

    #[test]
    fn test_concurrent() {
        let notifier = ChannelNotifier::default();

        let chain_a = ChainId::root(0);
        let chain_b = ChainId::root(1);

        let a_rec = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let b_rec = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let a_b_rec = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut rx_a = notifier.subscribe(vec![chain_a]);
        let mut rx_b = notifier.subscribe(vec![chain_b]);
        let mut rx_a_b = notifier.subscribe(vec![chain_a, chain_b]);

        let a_rec_clone = a_rec.clone();
        let b_rec_clone = b_rec.clone();
        let a_b_rec_clone = a_b_rec.clone();

        let notifier = Arc::new(notifier);

        std::thread::spawn(move || {
            while rx_a.blocking_recv().is_some() {
                a_rec_clone.fetch_add(1, Ordering::Relaxed);
            }
        });

        std::thread::spawn(move || {
            while rx_b.blocking_recv().is_some() {
                b_rec_clone.fetch_add(1, Ordering::Relaxed);
            }
        });

        std::thread::spawn(move || {
            while rx_a_b.blocking_recv().is_some() {
                a_b_rec_clone.fetch_add(1, Ordering::Relaxed);
            }
        });

        const NOTIFICATIONS_A: usize = 500;
        const NOTIFICATIONS_B: usize = 700;

        let a_notifier = notifier.clone();
        let handle_a = std::thread::spawn(move || {
            for _ in 0..NOTIFICATIONS_A {
                a_notifier.notify_chain(&chain_a, &());
            }
        });

        let handle_b = std::thread::spawn(move || {
            for _ in 0..NOTIFICATIONS_B {
                notifier.notify_chain(&chain_b, &());
            }
        });

        // finish sending all the messages
        handle_a.join().unwrap();
        handle_b.join().unwrap();

        // give some time for the messages to be received.
        std::thread::sleep(Duration::from_millis(100));

        assert_eq!(a_rec.load(Ordering::Relaxed), NOTIFICATIONS_A);
        assert_eq!(b_rec.load(Ordering::Relaxed), NOTIFICATIONS_B);
        assert_eq!(
            a_b_rec.load(Ordering::Relaxed),
            NOTIFICATIONS_A + NOTIFICATIONS_B
        );
    }

    #[test]
    fn test_eviction() {
        let notifier = ChannelNotifier::default();

        let chain_a = ChainId::root(0);
        let chain_b = ChainId::root(1);
        let chain_c = ChainId::root(2);
        let chain_d = ChainId::root(3);

        // Chain A -> Notify A, Notify B
        // Chain B -> Notify A, Notify B
        // Chain C -> Notify C
        // Chain D -> Notify A, Notify B, Notify C, Notify D

        let mut rx_a = notifier.subscribe(vec![chain_a, chain_b, chain_d]);
        let mut rx_b = notifier.subscribe(vec![chain_a, chain_b, chain_d]);
        let mut rx_c = notifier.subscribe(vec![chain_c, chain_d]);
        let mut rx_d = notifier.subscribe(vec![chain_d]);

        assert_eq!(notifier.inner.len(), 4);

        rx_c.close();
        notifier.notify_chain(&chain_c, &());
        assert_eq!(notifier.inner.len(), 3);

        rx_a.close();
        notifier.notify_chain(&chain_a, &());
        assert_eq!(notifier.inner.len(), 3);

        rx_b.close();
        notifier.notify_chain(&chain_b, &());
        assert_eq!(notifier.inner.len(), 2);

        notifier.notify_chain(&chain_a, &());
        assert_eq!(notifier.inner.len(), 1);

        rx_d.close();
        notifier.notify_chain(&chain_d, &());
        assert_eq!(notifier.inner.len(), 0);
    }
}

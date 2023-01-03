use dashmap::DashMap;
use linera_base::messages::ChainId;
use log::info;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;
use tonic::Status;

type NotificationResult<N> = Result<N, Status>;
type NotificationSender<N> = UnboundedSender<NotificationResult<N>>;

#[derive(Debug, Error)]
pub enum NotifierError {
    #[error("the requested chain does not exist")]
    ChainDoesNotExist,
}

/// The `Notifier` holds references to clients waiting to receive notifications
/// from the validator.
/// Clients will be evicted if their connections are terminated.
#[derive(Clone, Default)]
pub struct Notifier<N> {
    inner: DashMap<ChainId, Vec<NotificationSender<N>>>,
}

impl<N: Clone> Notifier<N> {
    /// Create a subscription given a collection of ChainIds and a sender to the client.
    pub fn create_subscription(&self, chains: Vec<ChainId>, sender: NotificationSender<N>) {
        for chain in chains {
            let mut existing_senders = self.inner.entry(chain).or_default();
            existing_senders.push(sender.clone());
        }
    }

    /// Notify all the clients waiting for a notification from a given chain.
    pub fn notify(&self, chain: &ChainId, notification: N) -> Result<(), NotifierError> {
        let senders_is_empty = {
            let mut senders_entry = self
                .inner
                .get_mut(chain)
                .ok_or(NotifierError::ChainDoesNotExist)?;
            let mut dead_senders = vec![];
            let senders = senders_entry.value_mut();

            for (index, sender) in senders.iter_mut().enumerate() {
                if sender.send(Ok(notification.clone())).is_err() {
                    dead_senders.push(index);
                }
            }

            for index in dead_senders.into_iter().rev() {
                info!("Removed dead sender for chain {:?}...", chain);
                senders.remove(index);
            }

            senders.is_empty()
        };

        if senders_is_empty {
            info!(
                "No subscribers for chain {:?}. Removing entry in notifier...",
                chain
            );
            self.inner.remove(chain);
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::{
        sync::{atomic::Ordering, Arc},
        time::Duration,
    };

    #[test]
    fn test_concurrent() {
        let notifier = Notifier::default();

        let chain_a = ChainId::root(0);
        let chain_b = ChainId::root(1);

        let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel();
        let (tx_a_b, mut rx_a_b) = tokio::sync::mpsc::unbounded_channel();

        let a_rec = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let b_rec = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let a_b_rec = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        notifier.create_subscription(vec![chain_a], tx_a);
        notifier.create_subscription(vec![chain_b], tx_b);
        notifier.create_subscription(vec![chain_a, chain_b], tx_a_b);

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
            let chain_a = chain_a;
            for _ in 0..NOTIFICATIONS_A {
                a_notifier.notify(&chain_a, ()).unwrap();
            }
        });

        let handle_b = std::thread::spawn(move || {
            let chain_b = chain_b;
            for _ in 0..NOTIFICATIONS_B {
                notifier.notify(&chain_b, ()).unwrap();
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
        let notifier = Notifier::default();

        let chain_a = ChainId::root(0);
        let chain_b = ChainId::root(1);
        let chain_c = ChainId::root(2);
        let chain_d = ChainId::root(3);

        let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel();
        let (tx_c, mut rx_c) = tokio::sync::mpsc::unbounded_channel();
        let (tx_d, mut rx_d) = tokio::sync::mpsc::unbounded_channel();

        // Chain A -> Notify A, Notify B
        // Chain B -> Notify A, Notify B
        // Chain C -> Notify C
        // Chain D -> Notify A, Notify B, Notify C, Notify D

        notifier.create_subscription(vec![chain_a, chain_b, chain_d], tx_a);
        notifier.create_subscription(vec![chain_a, chain_b, chain_d], tx_b);
        notifier.create_subscription(vec![chain_c, chain_d], tx_c);
        notifier.create_subscription(vec![chain_d], tx_d);

        assert_eq!(notifier.inner.len(), 4);

        rx_c.close();
        notifier.notify(&chain_c, ()).unwrap();
        assert_eq!(notifier.inner.len(), 3);

        rx_a.close();
        notifier.notify(&chain_a, ()).unwrap();
        assert_eq!(notifier.inner.len(), 3);

        rx_b.close();
        notifier.notify(&chain_b, ()).unwrap();
        assert_eq!(notifier.inner.len(), 2);

        notifier.notify(&chain_a, ()).unwrap();
        assert_eq!(notifier.inner.len(), 1);

        rx_d.close();
        notifier.notify(&chain_d, ()).unwrap();
        assert_eq!(notifier.inner.len(), 0);
    }
}

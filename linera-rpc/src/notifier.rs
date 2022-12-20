use crate::grpc_network::grpc::ChainId;
use dashmap::DashMap;
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

        if senders.is_empty() {
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

        let chain_a = ChainId { bytes: vec![0] };

        let chain_b = ChainId { bytes: vec![1] };

        let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel();
        let (tx_a_b, mut rx_a_b) = tokio::sync::mpsc::unbounded_channel();

        let a_rec = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let b_rec = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let a_b_rec = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        notifier.create_subscription(vec![chain_a.clone()], tx_a);
        notifier.create_subscription(vec![chain_b.clone()], tx_b);
        notifier.create_subscription(vec![chain_a.clone(), chain_b.clone()], tx_a_b);

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
            let chain_a = chain_a.clone();
            for _ in 0..NOTIFICATIONS_A {
                a_notifier.notify(&chain_a, ()).unwrap();
            }
        });

        let handle_b = std::thread::spawn(move || {
            let chain_b = chain_b.clone();
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
}

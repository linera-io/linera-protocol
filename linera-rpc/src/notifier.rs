use crate::grpc_network::grpc::ChainId;
use dashmap::DashMap;
use std::sync::Arc;
use log::warn;
use tokio::sync::mpsc::UnboundedSender;
use thiserror::Error;
use tonic::Status;

type NotificationResult<N> = Result<N, Status>;
type NotificationSender<N> = UnboundedSender<NotificationResult<N>>;

#[derive(Debug, Error)]
pub enum NotifierError {
    #[error("the requested chain does not exist")]
    ChainDoesNotExist
}

/// The `Notifier` holds references to clients waiting to receive notifications
/// from the validator.
/// The current implementation is unbounded in size - that is there is no eviction
/// policy for stale clients.
#[derive(Clone, Default)]
pub struct Notifier<N> {
    inner: DashMap<ChainId, Vec<Arc<NotificationSender<N>>>>
}

impl<N: Clone> Notifier<N> {
    /// Create a subscription given a collection of ChainIds and a sender to the client.
    pub fn create_subscription(&self, chains: Vec<ChainId>, sender: NotificationSender<N>) {
        let sender_arc = Arc::new(sender);

        for chain in chains {
            let mut existing_senders = self.inner
                .entry(chain)
                .or_insert(Vec::new());

            existing_senders.push(sender_arc.clone());
        }
    }

    /// Notify all the clients waiting for a notification from a given chain.
    pub fn notify(&self, chain: &ChainId, notification: N) -> Result<(), NotifierError> {
        let senders = self.inner.get(chain).ok_or(NotifierError::ChainDoesNotExist)?;

        for sender in senders.value() {
            if let Err(_) = sender.send(Ok(notification.clone())) {
                warn!("Sender {:?} is closed, skipping...", sender);
                // todo remove closed senders
            }
        }

        Ok(())
    }
}

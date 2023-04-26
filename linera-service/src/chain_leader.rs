// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::{lock::Mutex, StreamExt};
use linera_core::{
    client::{ChainClient, ValidatorNodeProvider},
    tracker::NotificationTracker,
    worker::Reason,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use std::{ops::DerefMut, sync::Arc};
use tracing::{debug, warn};

/// A `ChainLeader` is a process that listens to notifications from validators and reacts appropriately.
pub struct ChainLeader<P, S> {
    client: Arc<Mutex<ChainClient<P, S>>>,
}

impl<P, S> ChainLeader<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Creates a new instance of the node service given a client chain.
    pub fn new(client: Arc<Mutex<ChainClient<P, S>>>) -> Self {
        Self { client }
    }

    /// Runs the chain leader.
    pub async fn run<C, F>(self, mut context: C, wallet_updater: F) -> Result<(), anyhow::Error>
    where
        for<'a> F:
            (Fn(&'a mut C, &'a mut ChainClient<P, S>) -> futures::future::BoxFuture<'a, ()>) + Send,
    {
        let mut notification_stream = self.client.lock().await.listen().await?;
        let mut tracker = NotificationTracker::default();
        while let Some(notification) = notification_stream.next().await {
            debug!("Received notification: {:?}", notification);
            let mut client = self.client.lock().await;
            if tracker.insert(notification.clone()) {
                if let Err(e) = client.synchronize_and_recompute_balance().await {
                    warn!(
                        "Failed to synchronize and recompute balance for notification {:?} \
                            with error: {:?}",
                        notification, e
                    );
                    // If synchronization failed there is nothing to update validators
                    // about.
                    continue;
                }
                match &notification.reason {
                    Reason::NewBlock { .. } => {
                        if let Err(e) = client.update_validators_about_local_chain().await {
                            warn!(
                                "Failed to update validators about the local chain after \
                                         receiving notification {:?} with error: {:?}",
                                notification, e
                            );
                        }
                    }
                    Reason::NewMessage { .. } => {
                        if let Err(e) = client.process_inbox().await {
                            warn!(
                                "Failed to process inbox after receiving new message: {:?} \
                                    with error: {:?}",
                                notification, e
                            );
                        }
                    }
                }
                wallet_updater(&mut context, client.deref_mut()).await;
            }
        }

        Ok(())
    }
}

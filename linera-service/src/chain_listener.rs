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
use std::{ops::DerefMut, sync::Arc, time::Duration};
use structopt::StructOpt;
use tracing::{info, warn};

#[derive(Debug, Clone, StructOpt)]
pub struct ChainListenerConfig {
    /// Wait before processing any notification (useful for testing).
    #[structopt(long = "listener-delay-before-ms", default_value = "0")]
    pub(crate) delay_before_ms: u64,

    /// Wait after processing any notification (useful for rate limiting).
    #[structopt(long = "listener-delay-after-ms", default_value = "0")]
    pub(crate) delay_after_ms: u64,
}

/// A `ChainListener` is a process that listens to notifications from validators and reacts appropriately.
pub struct ChainListener<P, S> {
    config: ChainListenerConfig,
    client: Arc<Mutex<ChainClient<P, S>>>,
}

impl<P, S> ChainListener<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Creates a new instance of the node service given a client chain.
    pub fn new(config: ChainListenerConfig, client: Arc<Mutex<ChainClient<P, S>>>) -> Self {
        Self { config, client }
    }

    /// Runs the chain leader.
    pub async fn run<C, F>(self, mut context: C, wallet_updater: F) -> Result<(), anyhow::Error>
    where
        for<'a> F:
            (Fn(&'a mut C, &'a mut ChainClient<P, S>) -> futures::future::BoxFuture<'a, ()>) + Send,
    {
        let mut notification_stream = ChainClient::listen(self.client.clone()).await?;
        let mut tracker = NotificationTracker::default();
        while let Some(notification) = notification_stream.next().await {
            if tracker.insert(notification.clone()) {
                info!("Received new notification: {:?}", notification);
                if self.config.delay_before_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(self.config.delay_before_ms)).await;
                }
                {
                    let mut client = self.client.lock().await;
                    match &notification.reason {
                        Reason::NewBlock { .. } => {
                            if let Err(e) = client.update_validators().await {
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

                if self.config.delay_after_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(self.config.delay_after_ms)).await;
                }
            }
        }

        Ok(())
    }
}

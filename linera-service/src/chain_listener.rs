// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::{future, StreamExt};
use linera_core::{
    client::{ChainClient, ValidatorNodeProvider},
    tracker::NotificationTracker,
    worker::{Notification, Reason},
};
use linera_storage::Store;
use linera_views::views::ViewError;
use std::{collections::HashMap, time::Duration};
use structopt::StructOpt;
use tracing::{info, warn};

use crate::node_service::ClientMap;

#[derive(Debug, Clone, StructOpt)]
pub struct ChainListenerConfig {
    /// Wait before processing any notification (useful for testing).
    #[structopt(long = "listener-delay-before-ms", default_value = "0")]
    pub(crate) delay_before_ms: u64,

    /// Wait after processing any notification (useful for rate limiting).
    #[structopt(long = "listener-delay-after-ms", default_value = "0")]
    pub(crate) delay_after_ms: u64,
}

/// A `ChainListener` is a process that listens to notifications from validators and reacts
/// appropriately.
pub struct ChainListener<P, S> {
    config: ChainListenerConfig,
    clients: ClientMap<P, S>,
}

impl<P, S> ChainListener<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Creates a new chain listener given a client chain.
    pub(crate) fn new(config: ChainListenerConfig, clients: ClientMap<P, S>) -> Self {
        Self { config, clients }
    }

    /// Runs the chain listener.
    pub async fn run<C, F>(self, mut context: C, wallet_updater: F) -> Result<(), anyhow::Error>
    where
        for<'a> F:
            (Fn(&'a mut C, &'a mut ChainClient<P, S>) -> futures::future::BoxFuture<'a, ()>) + Send,
    {
        let mut streams = HashMap::new();
        let clients: Vec<_> = {
            let map_guard = self.clients.map_lock().await;
            map_guard
                .iter()
                .map(|(chain_id, client)| (*chain_id, client.clone()))
                .collect()
        };
        for (chain_id, client) in clients {
            let notification_stream = ChainClient::listen(client.clone()).await?;
            let mut tracker = NotificationTracker::default();
            streams.insert(
                chain_id,
                notification_stream
                    .filter(move |notification| future::ready(tracker.insert(notification.clone())))
                    .then(move |notification| {
                        info!("Received new notification: {:?}", notification);
                        let client = client.clone();
                        Box::pin(async move {
                            if self.config.delay_before_ms > 0 {
                                tokio::time::sleep(Duration::from_millis(
                                    self.config.delay_before_ms,
                                ))
                                .await;
                            }
                            {
                                let mut client = client.lock().await;
                                Self::handle_notification(&mut *client, notification).await;
                            }
                            if self.config.delay_after_ms > 0 {
                                tokio::time::sleep(Duration::from_millis(
                                    self.config.delay_after_ms,
                                ))
                                .await;
                            }
                            client.clone()
                        })
                    }),
            );
        }

        while let (Some(client), _, _) =
            future::select_all(streams.values_mut().map(StreamExt::next)).await
        {
            let mut client = client.lock().await;
            wallet_updater(&mut context, &mut *client).await;
        }

        Ok(())
    }

    async fn handle_notification(client: &mut ChainClient<P, S>, notification: Notification) {
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
            Reason::NewIncomingMessage { .. } => {
                if let Err(e) = client.process_inbox().await {
                    warn!(
                        "Failed to process inbox after receiving new message: {:?} \
                        with error: {:?}",
                        notification, e
                    );
                }
            }
        }
    }
}

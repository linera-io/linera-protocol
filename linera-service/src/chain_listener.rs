// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::{future, lock::Mutex, StreamExt};
use linera_base::identifiers::ChainId;
use linera_core::{
    client::{ChainClient, ValidatorNodeProvider},
    tracker::NotificationTracker,
    worker::{Notification, Reason},
};
use linera_storage::Store;
use linera_views::views::ViewError;
use std::{collections::HashMap, sync::Arc, time::Duration};
use structopt::StructOpt;
use tokio_stream::Stream;
use tracing::{info, warn};

use crate::{config::WalletState, node_service::ClientMap};

type ClientStream<P, S> = Box<dyn Stream<Item = Arc<Mutex<ChainClient<P, S>>>> + Send + Unpin>;

#[derive(Debug, Clone, StructOpt)]
pub struct ChainListenerConfig {
    /// Wait before processing any notification (useful for testing).
    #[structopt(long = "listener-delay-before-ms", default_value = "0")]
    pub(crate) delay_before_ms: u64,

    /// Wait after processing any notification (useful for rate limiting).
    #[structopt(long = "listener-delay-after-ms", default_value = "0")]
    pub(crate) delay_after_ms: u64,
}

pub trait ClientContext<P: ValidatorNodeProvider> {
    fn wallet_state(&self) -> &WalletState;

    fn make_chain_client<S>(
        &self,
        storage: S,
        chain_id: impl Into<Option<ChainId>>,
    ) -> ChainClient<P, S>;
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
    pub async fn run<C, F>(
        self,
        mut context: C,
        wallet_updater: F,
        storage: S,
    ) -> Result<(), anyhow::Error>
    where
        for<'a> F:
            (Fn(&'a mut C, &'a mut ChainClient<P, S>) -> futures::future::BoxFuture<'a, ()>) + Send,
        C: ClientContext<P>,
    {
        let mut streams = HashMap::new();
        self.update_streams(&mut streams, &mut context, &storage)
            .await?;

        while let (Some(client), _, _) =
            future::select_all(streams.values_mut().map(StreamExt::next)).await
        {
            let mut client = client.lock().await;
            wallet_updater(&mut context, &mut *client).await;
            self.update_streams(&mut streams, &mut context, &storage)
                .await?;
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

    async fn update_streams<C>(
        &self,
        streams: &mut HashMap<ChainId, ClientStream<P, S>>,
        context: &mut C,
        storage: &S,
    ) -> Result<(), anyhow::Error>
    where
        C: ClientContext<P>,
    {
        let new_clients: Vec<_> = {
            let mut map_guard = self.clients.map_lock().await;
            for chain_id in context.wallet_state().own_chain_ids() {
                map_guard.entry(chain_id).or_insert_with(|| {
                    let client = context.make_chain_client(storage.clone(), chain_id);
                    Arc::new(Mutex::new(client))
                });
            }
            map_guard
                .iter()
                .filter(|(chain_id, _)| !streams.contains_key(chain_id))
                .map(|(chain_id, client)| (*chain_id, client.clone()))
                .collect()
        };
        let delay_before_ms = self.config.delay_before_ms;
        let delay_after_ms = self.config.delay_after_ms;
        let new_streams = future::join_all(new_clients.into_iter().map(
            |(chain_id, client)| async move {
                {
                    // Process the inbox: For messages that are already there we won't receive a
                    // notification.
                    let mut guard = client.lock().await;
                    guard.synchronize_from_validators().await?;
                    guard.process_inbox().await?;
                }
                let notification_stream = ChainClient::listen(client.clone()).await?;
                let mut tracker = NotificationTracker::default();
                let stream = notification_stream
                    .filter(move |notification| future::ready(tracker.insert(notification.clone())))
                    .then(move |notification| {
                        info!("Received new notification: {:?}", notification);
                        let client = client.clone();
                        Box::pin(async move {
                            if delay_before_ms > 0 {
                                tokio::time::sleep(Duration::from_millis(delay_before_ms)).await;
                            }
                            {
                                let mut client = client.lock().await;
                                Self::handle_notification(&mut *client, notification).await;
                            }
                            if delay_after_ms > 0 {
                                tokio::time::sleep(Duration::from_millis(delay_after_ms)).await;
                            }
                            client.clone()
                        })
                    });
                Ok((chain_id, stream))
            },
        ))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
        for (chain_id, stream) in new_streams {
            streams.insert(chain_id, Box::new(stream));
        }
        Ok(())
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types to handle a set of running chain workers.
//!
//! The [`ChainWorkers`] type contains a set of running [`ChainWorkerActor`]s. It will
//! limit itself to a maximum number of active tasks, and will evict the least-recently
//! used chain worker when it is full and a new chain worker actor needs to be created.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use linera_base::{
    crypto::CryptoHash,
    identifiers::ChainId,
    time::timer::{sleep, timeout},
};
use linera_chain::{data_types::ExecutedBlock, types::Hashed};
use linera_storage::Storage;
use lru::LruCache;
use tokio::sync::mpsc;
use tracing::{warn, Instrument as _};

use crate::{
    chain_worker::{ChainWorkerActor, ChainWorkerConfig, ChainWorkerRequest, DeliveryNotifier},
    join_set_ext::{JoinSet, JoinSetExt},
    value_cache::ValueCache,
    worker::WorkerError,
};

/// A cache of running [`ChainWorkerActor`]s.
#[derive(Clone)]
pub struct ChainWorkers<StorageClient>
where
    StorageClient: Storage,
{
    /// One-shot channels to notify callers when messages of a particular chain have been
    /// delivered.
    delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
    /// The set of spawned [`ChainWorkerActor`] tasks.
    tasks: Arc<Mutex<JoinSet>>,
    /// The cache of running [`ChainWorkerActor`]s.
    cache: Arc<Mutex<LruCache<ChainId, ChainActorEndpoint<StorageClient>>>>,
}

/// The map of [`DeliveryNotifier`]s for each chain.
pub(crate) type DeliveryNotifiers = HashMap<ChainId, DeliveryNotifier>;

/// The sender endpoint for [`ChainWorkerRequest`]s.
pub type ChainActorEndpoint<StorageClient> =
    mpsc::UnboundedSender<ChainWorkerRequest<<StorageClient as Storage>::Context>>;

impl<StorageClient> ChainWorkers<StorageClient>
where
    StorageClient: Storage,
{
    /// Creates a new [`ChainWorkers`] that stores at most `limit` workers.
    pub fn new(limit: NonZeroUsize) -> Self {
        ChainWorkers {
            delivery_notifiers: Arc::default(),
            tasks: Arc::default(),
            cache: Arc::new(Mutex::new(LruCache::new(limit))),
        }
    }

    /// Obtains a [`ChainActorEndpoint`] to a [`ChainWorkerActor`] for a specific [`ChainId`], or
    /// creates a new one if it doesn't yet exist.
    pub async fn get_endpoint(
        &self,
        chain_id: ChainId,
    ) -> Result<
        Result<ChainActorEndpoint<StorageClient>, NewChainActorEndpoint<StorageClient>>,
        WorkerError,
    > {
        match self.try_get_existing_endpoint(chain_id) {
            Ok(endpoint) => Ok(Ok(endpoint)),
            Err(MissingEndpointError { cache_is_full }) => {
                if cache_is_full {
                    self.stop_one().await?;
                }

                let (sender, receiver) = mpsc::unbounded_channel();
                self.cache.lock().unwrap().push(chain_id, sender.clone());

                let delivery_notifier = self
                    .delivery_notifiers
                    .lock()
                    .unwrap()
                    .entry(chain_id)
                    .or_default()
                    .clone();

                Ok(Err(NewChainActorEndpoint {
                    chain_id,
                    delivery_notifier,
                    tasks: self.tasks.clone(),
                    sender,
                    receiver,
                }))
            }
        }
    }

    /// Attempts to get a [`ChainActorEndpoint`] for a chain worker actor that's already running.
    fn try_get_existing_endpoint(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainActorEndpoint<StorageClient>, MissingEndpointError> {
        let mut cache = self.cache.lock().unwrap();

        if let Some(endpoint) = cache.get(&chain_id) {
            Ok(endpoint.clone())
        } else {
            Err(MissingEndpointError {
                cache_is_full: cache.len() >= usize::from(cache.cap()),
            })
        }
    }

    /// Stops a single chain worker, opening up a slot for a new chain worker to be added.
    async fn stop_one(&self) -> Result<(), WorkerError> {
        timeout(Duration::from_secs(3), async move {
            loop {
                let evicted = {
                    let mut cache = self.cache.lock().unwrap();
                    let entry_to_evict = cache
                        .iter()
                        .rev()
                        .find(|(_, candidate_endpoint)| candidate_endpoint.strong_count() <= 1);

                    if let Some((&chain_to_evict, _)) = entry_to_evict {
                        cache.pop(&chain_to_evict);
                        self.clean_up_finished_chain_workers(&*cache);
                        true
                    } else {
                        false
                    }
                };

                if evicted {
                    break;
                } else {
                    sleep(Duration::from_millis(250)).await;
                    warn!("No chain worker candidates found for eviction, retrying...");
                }
            }
        })
        .await
        .map_err(|_| WorkerError::FullChainWorkerCache)
    }

    /// Cleans up any delivery notifiers for any chain workers that have stopped.
    fn clean_up_finished_chain_workers(
        &self,
        active_chain_workers: &LruCache<ChainId, ChainActorEndpoint<StorageClient>>,
    ) {
        self.tasks.lock().unwrap().reap_finished_tasks();

        self.delivery_notifiers
            .lock()
            .unwrap()
            .retain(|chain_id, notifier| {
                !notifier.is_empty() || active_chain_workers.contains(chain_id)
            });
    }

    /// Clears the set, forcing all running [`ChainWorkerActor`]s to stop.
    #[cfg(test)]
    pub(crate) fn stop_all(&self) {
        self.cache.lock().unwrap().clear();
    }
}

/// A [`ChainActorEndpoint`] for an actor that has not started yet.
pub struct NewChainActorEndpoint<StorageClient>
where
    StorageClient: Storage,
{
    chain_id: ChainId,
    delivery_notifier: DeliveryNotifier,
    tasks: Arc<Mutex<JoinSet>>,
    sender: ChainActorEndpoint<StorageClient>,
    receiver: mpsc::UnboundedReceiver<ChainWorkerRequest<StorageClient::Context>>,
}

impl<StorageClient> NewChainActorEndpoint<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    /// Consumes this [`NewChainActorEndpoint`] in order to start its [`ChainWorkerActor`]
    /// task.
    ///
    /// Returns the [`ChainActorEndpoint`] for the new [`ChainWorkerActor`].
    pub async fn start_actor(
        self,
        chain_worker_config: ChainWorkerConfig,
        storage: StorageClient,
        executed_block_cache: Arc<ValueCache<CryptoHash, Hashed<ExecutedBlock>>>,
        tracked_chains: Option<Arc<RwLock<HashSet<ChainId>>>>,
    ) -> Result<ChainActorEndpoint<StorageClient>, WorkerError> {
        let actor = ChainWorkerActor::load(
            chain_worker_config,
            storage,
            executed_block_cache,
            tracked_chains,
            self.delivery_notifier,
            self.chain_id,
        )
        .await?;

        self.tasks
            .lock()
            .unwrap()
            .spawn_task(actor.run(self.receiver).in_current_span());

        Ok(self.sender)
    }
}

/// An error for when an endpoint to a desired [`ChainActorWorker`] is not available in
/// the cache.
pub struct MissingEndpointError {
    /// Whether the cache is full.
    cache_is_full: bool,
}

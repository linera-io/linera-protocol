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
};

use linera_base::{crypto::CryptoHash, identifiers::ChainId};
use linera_chain::{data_types::ExecutedBlock, types::Hashed};
use linera_storage::Storage;
use lru::LruCache;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tracing::Instrument as _;

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
    cache: Arc<Mutex<LruCache<ChainId, ChainRequestSender<StorageClient>>>>,
    /// Active endpoint permits.
    active_endpoints: Arc<Semaphore>,
}

/// The map of [`DeliveryNotifier`]s for each chain.
pub(crate) type DeliveryNotifiers = HashMap<ChainId, DeliveryNotifier>;

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
            active_endpoints: Arc::new(Semaphore::new(limit.get())),
        }
    }

    /// Obtains a [`ChainActorEndpoint`] to a [`ChainWorkerActor`] for a specific [`ChainId`], or
    /// creates a new one if it doesn't yet exist.
    pub async fn get_endpoint(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainActorEndpoint<StorageClient>, NewChainActorEndpoint<StorageClient>> {
        let permit = self
            .active_endpoints
            .clone()
            .acquire_owned()
            .await
            .expect("`active_endpoints` semaphore should never be closed");

        self.try_get_existing_endpoint(chain_id, permit).map_err(
            |MissingEndpointError {
                 cache_is_full,
                 permit,
             }| {
                if cache_is_full {
                    self.stop_one();
                }

                self.create_new_endpoint(chain_id, permit)
            },
        )
    }

    /// Attempts to get a [`ChainActorEndpoint`] for a chain worker actor that's already running.
    fn try_get_existing_endpoint(
        &self,
        chain_id: ChainId,
        permit: OwnedSemaphorePermit,
    ) -> Result<ChainActorEndpoint<StorageClient>, MissingEndpointError> {
        let mut cache = self.cache.lock().unwrap();

        if let Some(sender) = cache.get(&chain_id) {
            Ok(ChainActorEndpoint::new(sender.clone(), permit))
        } else {
            Err(MissingEndpointError {
                cache_is_full: cache.len() >= usize::from(cache.cap()),
                permit,
            })
        }
    }

    /// Creates a new [`ChainActorEndpoint`], inserts it in the cache, and returns a
    /// [`NewChainActorEndpoint`] ready to start the chain worker actor task.
    fn create_new_endpoint(
        &self,
        chain_id: ChainId,
        permit: OwnedSemaphorePermit,
    ) -> NewChainActorEndpoint<StorageClient> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.cache.lock().unwrap().push(chain_id, sender.clone());

        let delivery_notifier = self
            .delivery_notifiers
            .lock()
            .unwrap()
            .entry(chain_id)
            .or_default()
            .clone();

        NewChainActorEndpoint {
            chain_id,
            delivery_notifier,
            tasks: self.tasks.clone(),
            sender: ChainActorEndpoint::new(sender, permit),
            receiver,
        }
    }

    /// Stops a single chain worker, opening up a slot for a new chain worker to be added.
    fn stop_one(&self) {
        let mut cache = self.cache.lock().unwrap();

        let (&chain_to_evict, _) = cache
            .iter()
            .rev()
            .find(|(_, candidate_endpoint)| candidate_endpoint.strong_count() <= 1)
            .expect("`stop_one` should only be called while holding a permit for an endpoint");

        cache.pop(&chain_to_evict);
        self.clean_up_finished_chain_workers(&*cache);
    }

    /// Cleans up any delivery notifiers for any chain workers that have stopped.
    fn clean_up_finished_chain_workers(
        &self,
        active_chain_workers: &LruCache<ChainId, ChainRequestSender<StorageClient>>,
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

/// The sender endpoint for [`ChainWorkerRequest`]s.
pub type ChainRequestSender<StorageClient> =
    mpsc::UnboundedSender<ChainWorkerRequest<<StorageClient as Storage>::Context>>;

/// An endpoint to communicate with a [`ChainWorkerActor`].
pub struct ChainActorEndpoint<StorageClient>
where
    StorageClient: Storage,
{
    sender: ChainRequestSender<StorageClient>,
    _permit: OwnedSemaphorePermit,
}

impl<StorageClient> ChainActorEndpoint<StorageClient>
where
    StorageClient: Storage,
{
    /// Creates a new [`ChainActorEndpoint`] instance.
    pub fn new(sender: ChainRequestSender<StorageClient>, permit: OwnedSemaphorePermit) -> Self {
        ChainActorEndpoint {
            sender,
            _permit: permit,
        }
    }

    /// Sends a [`ChainWorkerRequest`] to the [`ChainWorkerActor`].
    pub fn send(
        &self,
        request: ChainWorkerRequest<StorageClient::Context>,
    ) -> Result<(), mpsc::error::SendError<ChainWorkerRequest<StorageClient::Context>>> {
        self.sender.send(request)
    }
}

/// A wrapper around a [`ChainActorEndpoint`] for an actor that has not started yet.
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
    /// The permit for using the desired endpoint, which will be reused if the endpoint is created.
    permit: OwnedSemaphorePermit,
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A handle providing RwLock-based access to a chain worker's state.

use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use linera_base::{
    data_types::{BlockHeight, Timestamp},
    identifiers::ChainId,
    time::Duration,
};
use linera_execution::{QueryContext, ServiceRuntimeEndpoint, ServiceSyncRuntime};
use linera_storage::Storage;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use super::{config::ChainWorkerConfig, state::ChainWorkerState};

/// A handle to a chain worker's state, protected by an `RwLock`.
///
/// Concurrent reads acquire the read lock; mutations acquire the write lock. Tokio's `RwLock` is
/// write-preferring, which guarantees that mutations don't starve if there are many reads.
pub(crate) struct ChainHandle<S: Storage> {
    state: Arc<RwLock<ChainWorkerState<S>>>,
    last_access: Arc<AtomicU64>,
    service_runtime_task: Mutex<Option<web_thread_pool::Task<()>>>,
}

/// A write guard that automatically rolls back uncommitted chain state changes on drop.
///
/// This ensures cancellation safety: if a write operation's future is dropped before
/// completion, any uncommitted state changes are rolled back rather than leaked.
pub(crate) struct RollbackGuard<S: Storage + Clone + 'static>(
    tokio::sync::OwnedRwLockWriteGuard<ChainWorkerState<S>>,
);

impl<S: Storage + Clone + 'static> Deref for RollbackGuard<S> {
    type Target = ChainWorkerState<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S: Storage + Clone + 'static> DerefMut for RollbackGuard<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S: Storage + Clone + 'static> Drop for RollbackGuard<S> {
    fn drop(&mut self) {
        self.0.rollback();
    }
}

/// Actor that manages a long-lived service runtime in a background thread.
pub(crate) struct ServiceRuntimeActor {
    pub(crate) task: web_thread_pool::Task<()>,
    pub(crate) endpoint: ServiceRuntimeEndpoint,
}

impl ServiceRuntimeActor {
    /// Spawns a blocking task to execute the service runtime actor.
    pub(crate) async fn spawn(
        chain_id: ChainId,
        thread_pool: &linera_execution::ThreadPool,
    ) -> Self {
        let (execution_state_sender, incoming_execution_requests) =
            futures::channel::mpsc::unbounded();
        let (runtime_request_sender, runtime_request_receiver) = std::sync::mpsc::channel();

        Self {
            endpoint: ServiceRuntimeEndpoint {
                incoming_execution_requests,
                runtime_request_sender,
            },
            task: thread_pool
                .run((), move |()| async move {
                    ServiceSyncRuntime::new(
                        execution_state_sender,
                        QueryContext {
                            chain_id,
                            next_block_height: BlockHeight(0),
                            local_time: Timestamp::from(0),
                        },
                    )
                    .run(runtime_request_receiver)
                })
                .await,
        }
    }
}

impl<S: Storage + Clone + 'static> ChainHandle<S> {
    /// Creates a new `ChainHandle` wrapping the given worker state.
    ///
    /// If the config specifies a TTL for this handle's type (tracked vs untracked),
    /// a background keep-alive task is spawned that holds a strong `Arc` reference.
    /// When the handle has been idle for the TTL duration, the task exits and drops
    /// its reference. If no other strong references remain, the handle is deallocated.
    pub(crate) fn new_arc(
        state: ChainWorkerState<S>,
        service_runtime_task: Option<web_thread_pool::Task<()>>,
        is_tracked: bool,
        config: &ChainWorkerConfig,
    ) -> Arc<Self> {
        let now_micros = current_time_micros();
        let handle = Arc::new(ChainHandle {
            state: Arc::new(RwLock::new(state)),
            last_access: Arc::new(AtomicU64::new(now_micros)),
            service_runtime_task: Mutex::new(service_runtime_task),
        });
        let ttl = if is_tracked {
            config.sender_chain_ttl
        } else {
            config.ttl
        };
        if let Some(ttl) = ttl {
            Self::spawn_keep_alive(Arc::clone(&handle), ttl);
        }
        handle
    }

    /// Spawns a background task that keeps the handle alive for at least `ttl` after
    /// the last access. When the handle has been idle for the full TTL, the task exits
    /// and drops its strong reference, allowing deallocation.
    fn spawn_keep_alive(handle: Arc<Self>, ttl: Duration) {
        linera_base::Task::spawn(async move {
            loop {
                linera_base::time::timer::sleep(ttl).await;
                let last = handle.last_access.load(Ordering::Relaxed);
                let now = current_time_micros();
                let idle_micros = now.saturating_sub(last);
                let ttl_micros = u64::try_from(ttl.as_micros()).unwrap_or(u64::MAX);
                if idle_micros >= ttl_micros {
                    break;
                }
                // Touched recently — sleep for the remaining time.
                let remaining = Duration::from_micros(ttl_micros - idle_micros);
                linera_base::time::timer::sleep(remaining).await;
            }
            handle.shutdown().await;
        })
        .forget();
    }

    /// Updates the last-access timestamp.
    fn touch(&self) {
        self.last_access
            .store(current_time_micros(), Ordering::Relaxed);
    }

    /// Acquires a read lock, updating the last-access timestamp.
    pub(crate) async fn read(&self) -> OwnedRwLockReadGuard<ChainWorkerState<S>> {
        self.touch();
        self.state.clone().read_owned().await
    }

    /// Acquires a read lock, initializing the chain if needed.
    ///
    /// This method first acquires a read lock and checks if the chain is already known
    /// to be active. If not, it drops the read lock, acquires a write lock to initialize
    /// the chain, then re-acquires the read lock.
    pub(crate) async fn read_initialized(
        &self,
    ) -> Result<OwnedRwLockReadGuard<ChainWorkerState<S>>, crate::worker::WorkerError> {
        {
            let guard = self.read().await;
            if guard.knows_chain_is_active() {
                return Ok(guard);
            }
        }
        {
            let mut guard = self.write().await;
            guard.initialize_and_save_if_needed().await?;
        }
        Ok(self.read().await)
    }

    /// Acquires a write lock, updating the last-access timestamp.
    ///
    /// Returns a [`RollbackGuard`] that automatically rolls back uncommitted changes
    /// when dropped, ensuring cancellation safety.
    pub(crate) async fn write(&self) -> RollbackGuard<S> {
        self.touch();
        RollbackGuard(self.state.clone().write_owned().await)
    }

    /// Shuts down the service runtime, cleaning up resources.
    async fn shutdown(&self) {
        // Acquire write lock to wait for all outstanding guards to drain,
        // then clear the endpoint so the runtime task's channel closes.
        {
            let mut guard = RollbackGuard(self.state.clone().write_owned().await);
            guard.clear_service_runtime_endpoint();
        }
        let task = self.service_runtime_task.lock().unwrap().take();
        if let Some(task) = task {
            if let Err(err) = task.await {
                tracing::warn!(%err, "Failed to shut down service runtime");
            }
        }
    }
}

/// Returns current time in microseconds since the Unix epoch.
fn current_time_micros() -> u64 {
    linera_base::time::SystemTime::now()
        .duration_since(linera_base::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

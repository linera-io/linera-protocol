// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helpers for managing chain worker state behind an `Arc<RwLock<…>>`.

use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
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

/// Wraps a [`ChainWorkerState`] in an `Arc<RwLock<…>>` and spawns a keep-alive task
/// if a TTL is configured.
pub(crate) fn create_chain_worker<S: Storage + Clone + 'static>(
    state: ChainWorkerState<S>,
    is_tracked: bool,
    config: &ChainWorkerConfig,
) -> Arc<RwLock<ChainWorkerState<S>>> {
    let last_access = state.last_access_arc();
    let arc = Arc::new(RwLock::new(state));
    let ttl = if is_tracked {
        config.sender_chain_ttl
    } else {
        config.ttl
    };
    if let Some(ttl) = ttl {
        spawn_keep_alive(Arc::clone(&arc), last_access, ttl);
    }
    arc
}

/// Acquires a read lock, updating the last-access timestamp.
pub(crate) async fn read_lock<S: Storage + Clone + 'static>(
    state: &Arc<RwLock<ChainWorkerState<S>>>,
) -> OwnedRwLockReadGuard<ChainWorkerState<S>> {
    let guard = state.clone().read_owned().await;
    guard.touch();
    guard
}

/// Acquires a read lock, initializing the chain if needed.
///
/// First acquires a read lock and checks if the chain is already known to be active.
/// If not, drops the read lock, acquires a write lock to initialize the chain,
/// then re-acquires the read lock.
pub(crate) async fn read_lock_initialized<S: Storage + Clone + 'static>(
    state: &Arc<RwLock<ChainWorkerState<S>>>,
) -> Result<OwnedRwLockReadGuard<ChainWorkerState<S>>, crate::worker::WorkerError> {
    {
        let guard = read_lock(state).await;
        if guard.knows_chain_is_active() {
            return Ok(guard);
        }
    }
    {
        let mut guard = write_lock(state).await;
        guard.initialize_and_save_if_needed().await?;
    }
    Ok(read_lock(state).await)
}

/// Acquires a write lock, updating the last-access timestamp.
///
/// Returns a [`RollbackGuard`] that automatically rolls back uncommitted changes
/// when dropped, ensuring cancellation safety.
pub(crate) async fn write_lock<S: Storage + Clone + 'static>(
    state: &Arc<RwLock<ChainWorkerState<S>>>,
) -> RollbackGuard<S> {
    let guard = RollbackGuard(state.clone().write_owned().await);
    guard.touch();
    guard
}

/// Spawns a background task that keeps the chain state alive for at least `ttl`
/// after the last access. When the state has been idle for the full TTL, the task
/// shuts down the service runtime, drops its strong reference, and exits.
fn spawn_keep_alive<S: Storage + Clone + 'static>(
    state: Arc<RwLock<ChainWorkerState<S>>>,
    last_access: Arc<AtomicU64>,
    ttl: Duration,
) {
    linera_base::Task::spawn(async move {
        loop {
            linera_base::time::timer::sleep(ttl).await;
            let last = last_access.load(Ordering::Relaxed);
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
        // Shut down: acquire write lock to drain outstanding guards, then
        // clear the endpoint and await the runtime task.
        let task = {
            let mut guard = RollbackGuard(state.clone().write_owned().await);
            guard.clear_service_runtime()
        };
        if let Some(task) = task {
            if let Err(err) = task.await {
                tracing::warn!(%err, "Failed to shut down service runtime");
            }
        }
        drop(state);
    })
    .forget();
}

/// Returns current time in microseconds since the Unix epoch.
pub(crate) fn current_time_micros() -> u64 {
    linera_base::time::SystemTime::now()
        .duration_since(linera_base::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

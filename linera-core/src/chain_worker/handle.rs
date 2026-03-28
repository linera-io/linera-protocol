// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helpers for managing chain worker state behind an `Arc<RwLock<…>>`.
//!
//! Tokio's [`RwLock`] is write-preferring: once a writer is waiting, new readers
//! queue behind it. This prevents read-only queries from starving write operations
//! (block proposals, certificate handling, etc.).

use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

/// An atomically-updatable timestamp backed by microseconds since the Unix epoch.
///
/// This wraps the raw `AtomicU64` microsecond encoding so that call sites work
/// exclusively with [`Duration`] and never see the underlying representation.
pub(crate) struct AtomicTimestamp(AtomicU64);

impl AtomicTimestamp {
    /// Creates a new `AtomicTimestamp` set to the current time.
    pub(crate) fn now() -> Self {
        Self(AtomicU64::new(Self::current_micros()))
    }

    /// Updates the stored timestamp to the current time.
    pub(crate) fn store_now(&self) {
        self.0.store(Self::current_micros(), Ordering::Relaxed);
    }

    /// Returns how long has passed since the stored timestamp.
    pub(crate) fn elapsed(&self) -> Duration {
        let last = self.0.load(Ordering::Relaxed);
        let now = Self::current_micros();
        Duration::from_micros(now.saturating_sub(last))
    }

    fn current_micros() -> u64 {
        linera_base::time::SystemTime::now()
            .duration_since(linera_base::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }
}

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

/// The endpoint and background task for a long-lived service runtime.
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
                    // The dummy context is overwritten by `prepare_for_query`
                    // before the first actual query is executed.
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
    let chain_id = state.chain().chain_id();
    let arc = Arc::new(RwLock::new(state));
    let ttl = if is_tracked {
        config.sender_chain_ttl
    } else {
        config.ttl
    };
    if let Some(ttl) = ttl {
        spawn_keep_alive(chain_id, Arc::clone(&arc), last_access, ttl);
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
/// drops the state if it holds the only strong reference.
fn spawn_keep_alive<S: Storage + Clone + 'static>(
    chain_id: ChainId,
    mut state: Arc<RwLock<ChainWorkerState<S>>>,
    last_access: Arc<AtomicTimestamp>,
    ttl: Duration,
) {
    linera_base::Task::spawn(async move {
        loop {
            while let Some(remaining) = ttl
                .checked_sub(last_access.elapsed())
                .filter(|remaining| *remaining > Duration::ZERO)
            {
                // Touched recently — sleep for the remaining time.
                linera_base::time::timer::sleep(remaining).await;
            }
            // Idle long enough. Drop our strong reference if it's the only one.
            match Arc::try_unwrap(state) {
                Ok(rw_lock) => {
                    tracing::debug!(%chain_id, "Dropping chain worker");
                    // We have sole ownership — extract the state and
                    // shut down the service runtime gracefully.
                    let mut worker_state = RwLock::into_inner(rw_lock);
                    let task = worker_state.clear_service_runtime();
                    drop(worker_state);
                    if let Some(task) = task {
                        if let Err(err) = task.await {
                            tracing::warn!(%err, "Failed to shut down service runtime");
                        }
                    }
                    break;
                }
                Err(arc) => {
                    arc.read().await.touch();
                    state = arc;
                }
            }
        }
    })
    .forget();
}

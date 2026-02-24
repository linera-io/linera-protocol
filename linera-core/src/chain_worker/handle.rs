// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A handle providing RwLock-based access to a chain worker's state.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use linera_base::{
    data_types::{BlockHeight, Timestamp},
    identifiers::ChainId,
    time::Duration,
};
use linera_execution::{QueryContext, ServiceRuntimeEndpoint, ServiceSyncRuntime};
use linera_storage::Storage;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use super::state::ChainWorkerState;

/// A handle to a chain worker's state, protected by an `RwLock`.
///
/// Replaces the former channel-based `ChainWorkerActor`. Concurrent reads acquire the
/// read lock; mutations acquire the write lock. Tokio's `RwLock` is write-preferring,
/// providing the same scheduling semantics as the former actor's write-priority loop.
pub(crate) struct ChainHandle<S: Storage> {
    state: Arc<RwLock<ChainWorkerState<S>>>,
    last_access: Arc<AtomicU64>,
    #[allow(dead_code)] // Used by TTL sweep task
    service_runtime_task: Mutex<Option<web_thread_pool::Task<()>>>,
    #[allow(dead_code)] // Used by TTL sweep task
    is_tracked: bool,
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
    pub(crate) fn new(
        state: ChainWorkerState<S>,
        service_runtime_task: Option<web_thread_pool::Task<()>>,
        is_tracked: bool,
    ) -> Self {
        let now_micros = current_time_micros();
        ChainHandle {
            state: Arc::new(RwLock::new(state)),
            last_access: Arc::new(AtomicU64::new(now_micros)),
            service_runtime_task: Mutex::new(service_runtime_task),
            is_tracked,
        }
    }

    /// Updates the last-access timestamp.
    fn touch(&self) {
        self.last_access
            .store(current_time_micros(), Ordering::Relaxed);
    }

    /// Checks whether this handle has been idle beyond its TTL.
    #[allow(dead_code)] // Used by TTL sweep task
    pub(crate) fn is_expired(&self, ttl: Duration, sender_ttl: Duration) -> bool {
        let timeout = if self.is_tracked { sender_ttl } else { ttl };
        let timeout_micros = u64::try_from(timeout.as_micros()).unwrap_or(u64::MAX);
        let last = self.last_access.load(Ordering::Relaxed);
        let now = current_time_micros();
        now.saturating_sub(last) > timeout_micros
    }

    /// Acquires a read lock, updating the last-access timestamp.
    pub(crate) async fn read(&self) -> OwnedRwLockReadGuard<ChainWorkerState<S>> {
        self.touch();
        self.state.clone().read_owned().await
    }

    /// Acquires a write lock, updating the last-access timestamp.
    pub(crate) async fn write(&self) -> OwnedRwLockWriteGuard<ChainWorkerState<S>> {
        self.touch();
        self.state.clone().write_owned().await
    }

    /// Shuts down the service runtime task, if any.
    #[allow(dead_code)] // Used by TTL sweep task
    pub(crate) async fn shutdown_service_runtime(&self) -> Result<(), web_thread_pool::Error> {
        let task = self.service_runtime_task.lock().unwrap().take();
        if let Some(task) = task {
            task.await?;
        }
        Ok(())
    }
}

/// Returns current time in microseconds since the Unix epoch.
fn current_time_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

//! This module contains some helper types to prevent concurrent access to the same chain data.
//!
//! The [`ChainGuards`] type controls the active guards. It can be cheaply cloned and shared
//! between threads.
//!
//! The [`ChainGuard`] type is used to guard a single chain. There is always a single live instance
//! for a given chain, and new instances for the same chain can only be created when the previous
//! instance is dropped.

use dashmap::DashMap;
use linera_base::messages::ChainId;
use std::sync::{Arc, Weak};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// The internal map type.
///
/// Every chain ID is mapped to a weak reference to an asynchronous [`Mutex`].
///
/// Attempting to upgrade the weak reference allows checking if there's a live guard for that chain.
type ChainGuardMap = DashMap<ChainId, Weak<Mutex<()>>>;

/// Manager of [`ChainGuard`]s for chains.
///
/// The [`ChainGuard::guard`] method can be used to obtain a guard for a specific chain. The guard
/// is always guaranteed to be the only live guard for that chain.
#[derive(Clone, Debug, Default)]
pub struct ChainGuards {
    guards: Arc<ChainGuardMap>,
}

impl ChainGuards {
    /// Obtain a guard for a specified chain, waiting if there's already a live guard.
    ///
    /// Only one guard can be active for a chain, so if there's already a guard for the requested
    /// chain, this method will wait until it is able to obtain the guard.
    ///
    /// The lock used for the guard is stored in a shared [`ChainGuardMap`]. A weak reference is
    /// stored, because the goal is to remove the map entry as soon as possible, and the weak
    /// reference can only be upgraded if there's another attempt waiting to create a guard for
    /// the same chain.
    pub async fn guard(&mut self, chain_id: ChainId) -> ChainGuard {
        let guard = self.get_or_create_lock(chain_id);
        ChainGuard {
            chain_id,
            guards: self.guards.clone(),
            guard: Some(guard.lock_owned().await),
        }
    }

    /// Obtain the lock used for guarding a chain.
    ///
    /// When obtaining a lock, first a write lock to the map entry is obtained. If there is no
    /// entry, a new lock for that entry is created.
    ///
    /// Then, an attempt is made to upgrade the weak reference into a strong reference. If that
    /// succeeds, there's already a live guard for that chain, and that strong reference to the lock
    /// can be returned to wait until it's possible to create the guard.
    ///
    /// If upgrading the weak reference fails, then there is no more live guards, but the entry
    /// hasn't been removed yet. A new lock must be created and inserted in the entry.
    ///
    /// It's important that the returned lock is only locked after the write lock for the map entry
    /// is released at the end of this method, to avoid deadlocks. See [`ChainGuard::drop`] for
    /// more details.
    fn get_or_create_lock(&mut self, chain_id: ChainId) -> Arc<Mutex<()>> {
        let mut new_guard_holder = None;
        let mut guard_reference = self.guards.entry(chain_id).or_insert_with(|| {
            let (new_guard, weak_reference) = Self::create_new_mutex();
            new_guard_holder = Some(new_guard);
            weak_reference
        });
        guard_reference.upgrade().unwrap_or_else(|| {
            let (new_guard, weak_reference) = Self::create_new_mutex();
            *guard_reference = weak_reference;
            new_guard
        })
    }

    /// Creates a new [`Mutex`] in the heap, returning both a strong reference and a weak
    /// reference to it.
    fn create_new_mutex() -> (Arc<Mutex<()>>, Weak<Mutex<()>>) {
        let new_guard = Arc::new(Mutex::new(()));
        let weak_reference = Arc::downgrade(&new_guard);
        (new_guard, weak_reference)
    }
}

/// A guard for a specific chain.
///
/// Only one instance for a chain is guaranteed to exist at any given moment.
///
/// When the instance is dropped, the lock is released and a new instance can be created. If no new
/// instances are waiting to be created, the entry in the map is removed.
pub struct ChainGuard {
    chain_id: ChainId,
    guards: Arc<ChainGuardMap>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for ChainGuard {
    /// Release the lock and remove the entry from the map if possible.
    ///
    /// Before the releasing the lock, a weak reference to the lock is obtained. After releasing
    /// the lock, an attempt is made to upgrade the weak reference to a strong reference. If that
    /// succeeds, it indicates that there's a [`ChainGuards`] instance waiting to obtain the lock,
    /// so the entry in the [`ChainGuardMap`] is not removed.
    ///
    /// A race condition while obtaining the weak reference, releasing the lock, and attempting to
    /// upgrade the weak reference is guaranteed to not occur, because:
    ///
    /// 1. The steps are performed inside [`DashMap::remove_if`], which holds write lock to the
    ///    entry.
    /// 2. The [`ChainGuards::get_or_create_lock`] method's body is only executed after obtaining a
    ///    write lock to the entry.
    /// 3. The mutex is only locked in [`ChainGuards::guard`], which does not hold any locks to the
    ///    map.
    fn drop(&mut self) {
        self.guards.remove_if(&self.chain_id, |_, _| {
            let mutex = Arc::downgrade(OwnedMutexGuard::mutex(
                &self
                    .guard
                    .take()
                    .expect("Guard dropped before `Drop` implementation"),
            ));
            mutex.upgrade().is_none()
        });
    }
}

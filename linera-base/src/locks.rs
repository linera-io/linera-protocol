// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Custom synchronization locks with embedded logging.

use async_lock::{Mutex, MutexGuard, MutexGuardArc};
use std::{
    any::type_name,
    fmt::{self, Debug, Formatter},
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};

/// A mutex to be used in asynchronous tasks.
pub struct AsyncMutex<T> {
    name: Arc<str>,
    lock: Arc<Mutex<T>>,
}

impl<T> AsyncMutex<T> {
    /// Create a new [`AsyncMutex`] with the provided `name` to guard `data`.
    pub fn new(name: impl Into<String>, data: T) -> Self {
        AsyncMutex {
            name: name.into().into(),
            lock: Arc::new(Mutex::new(data)),
        }
    }

    /// Locks the [`AsyncMutex`] and returns an [`AsyncMutexGuard`] to access the underlying data.
    pub async fn lock(&self) -> AsyncMutexGuard<'_, T> {
        let name = self.name.clone();
        tracing::trace!(name = %self.name, "Locking");
        let guard = self.lock.lock().await;
        tracing::trace!(name = %self.name, "Locked");
        AsyncMutexGuard { name, guard }
    }

    /// Locks the [`AsyncMutex`] and returns an [`OwnedAsyncMutexGuard`] to access the underlying
    /// data.
    pub async fn lock_owned(&self) -> OwnedAsyncMutexGuard<T> {
        let name = self.name.clone();
        tracing::trace!(name = %self.name, "Locking");
        let guard = self.lock.clone().lock_arc().await;
        tracing::trace!(name = %self.name, "Locked");
        OwnedAsyncMutexGuard { name, guard }
    }

    /// Downgrades this [`AsyncMutex`], returning a handle that can be upgraded back to the
    /// [`AsyncMutex`] as long as there's at least one other reference to the same underlying
    /// instance.
    pub fn downgrade(&self) -> WeakAsyncMutex<T> {
        WeakAsyncMutex {
            name: Arc::downgrade(&self.name),
            lock: Arc::downgrade(&self.lock),
        }
    }
}

impl<T> Clone for AsyncMutex<T> {
    fn clone(&self) -> Self {
        AsyncMutex {
            name: self.name.clone(),
            lock: self.lock.clone(),
        }
    }
}

impl<T> Debug for AsyncMutex<T> {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        formatter
            .debug_struct("AsyncMutex")
            .field("name", &self.name)
            .field("lock", &format_args!("Arc<Mutex<{}>>", type_name::<T>()))
            .finish()
    }
}

/// A weak handle to an [`AsyncMutex`].
pub struct WeakAsyncMutex<T> {
    name: Weak<str>,
    lock: Weak<Mutex<T>>,
}

impl<T> WeakAsyncMutex<T> {
    /// Attempts to upgrade this [`WeakAsyncMutex`] into its respective [`AsyncMutex`].
    pub fn upgrade(&self) -> Option<AsyncMutex<T>> {
        Some(AsyncMutex {
            name: self.name.upgrade()?,
            lock: self.lock.upgrade()?,
        })
    }

    /// Returns `true` if this weak reference can no longer be upgraded.
    pub fn no_longer_upgradable(&self) -> bool {
        // Both `Weak` handles below may race and have different strong counts, but if any one of
        // them reaches zero, it's impossible to upgrade because the referenced data has been
        // dropped.
        self.name.strong_count() == 0 || self.lock.strong_count() == 0
    }
}

impl<T> Debug for WeakAsyncMutex<T> {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        formatter
            .debug_struct("WeakAsyncMutex")
            .field("name", &format_args!("Weak<str>"))
            .field("lock", &format_args!("Weak<Mutex<{}>>", type_name::<T>()))
            .finish()
    }
}

/// A guard that unlocks its respective [`AsyncMutex`] when dropped.
pub struct AsyncMutexGuard<'guard, T> {
    name: Arc<str>,
    guard: MutexGuard<'guard, T>,
}

impl<T> Drop for AsyncMutexGuard<'_, T> {
    fn drop(&mut self) {
        tracing::trace!(name = %self.name, "Unlocking");
    }
}

impl<'guard, T> Deref for AsyncMutexGuard<'guard, T> {
    type Target = MutexGuard<'guard, T>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'guard, T> DerefMut for AsyncMutexGuard<'guard, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

/// An owned guard that unlocks its respective [`AsyncMutex`] when dropped.
pub struct OwnedAsyncMutexGuard<T> {
    name: Arc<str>,
    guard: MutexGuardArc<T>,
}

impl<T> Drop for OwnedAsyncMutexGuard<T> {
    fn drop(&mut self) {
        tracing::trace!(name = %self.name, "Unlocking");
    }
}

impl<T> Deref for OwnedAsyncMutexGuard<T> {
    type Target = MutexGuardArc<T>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> DerefMut for OwnedAsyncMutexGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

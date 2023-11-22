// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Custom synchronization locks with embedded logging.

use async_lock::{Mutex, MutexGuard};
use std::{
    any::type_name,
    fmt::{self, Debug, Formatter},
    ops::{Deref, DerefMut},
    sync::Arc,
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

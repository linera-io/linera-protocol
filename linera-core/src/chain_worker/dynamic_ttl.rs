// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A shared, atomically-adjustable TTL for chain worker keep-alive tasks.

use std::sync::atomic::{AtomicU64, Ordering};

use linera_base::time::Duration;

/// A TTL value that can be shared across chain workers and adjusted at runtime.
///
/// The memory monitor in `linera-service` reduces the TTL when the process is under
/// memory pressure, causing idle workers to be evicted sooner.
pub struct DynamicTtl {
    /// The configured base TTL (the value used when there is no memory pressure).
    base_micros: u64,
    /// The currently active TTL, stored as microseconds. May be lower than `base_micros`
    /// when the memory monitor has reduced it.
    current_micros: AtomicU64,
}

impl DynamicTtl {
    /// Creates a new `DynamicTtl` with the given base duration.
    pub fn new(base: Duration) -> Self {
        let micros = base.as_micros() as u64;
        Self {
            base_micros: micros,
            current_micros: AtomicU64::new(micros),
        }
    }

    /// Returns the currently active TTL.
    pub fn current(&self) -> Duration {
        Duration::from_micros(self.current_micros.load(Ordering::Relaxed))
    }

    /// Returns the configured base TTL (before any memory-pressure adjustment).
    pub fn base(&self) -> Duration {
        Duration::from_micros(self.base_micros)
    }

    /// Sets the active TTL. Clamped to at most the base TTL.
    pub fn set(&self, ttl: Duration) {
        let micros = (ttl.as_micros() as u64).min(self.base_micros);
        self.current_micros.store(micros, Ordering::Relaxed);
    }

    /// Resets the active TTL back to the base value.
    pub fn reset(&self) {
        self.current_micros
            .store(self.base_micros, Ordering::Relaxed);
    }
}

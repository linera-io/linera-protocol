// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Caching utilities for the Linera protocol.
//!
//! ## Hash-consing and the "one allocation per content" invariant
//!
//! [`ValueCache`] is the canonical home for content-addressed immutable data
//! (also known as hash-consed data) such as `Block`, `Blob`, and
//! `ConfirmedBlockCertificate`. For such types the cache guarantees that at
//! most one allocation exists per distinct content at any time, and all
//! consumers share the same `Arc<T>`.
//!
//! The guarantee is implemented by combining two structures:
//!
//! - A bounded `quick_cache` (S3-FIFO eviction) for hot-path lookups.
//! - A lock-free `papaya::HashMap<K, Weak<V>>` weak index that survives bounded
//!   eviction. If the bounded cache evicts an entry while a consumer still
//!   holds an `Arc`, re-requesting the same key returns the existing
//!   allocation instead of creating a duplicate.
//!
//! A background task periodically sweeps dead `Weak` entries from the index to
//! prevent unbounded growth.
//!
//! For the invariant to hold, all inserts of hash-consed values must go
//! through the cache: [`ValueCache::intern`] for values that may not be in storage,
//! [`ValueCache::insert`] or [`ValueCache::insert_hashed`] once storage is known to
//! hold them.
//! The [`Arc`] newtype enforces this structurally: it has no public constructor,
//! so callers cannot bypass the cache by calling `std::sync::Arc::new` directly.

#![deny(missing_docs)]

mod arc;
mod unique_value_cache;
mod value_cache;

pub use arc::Arc;
pub use unique_value_cache::UniqueValueCache;
pub use value_cache::{ValueCache, DEFAULT_CLEANUP_INTERVAL_SECS};

/// Registers every metric this crate declares.
///
/// Without this, a metric is only exported after the code path that observes it has run, so a
/// rarely-taken path leaves its panels blank and makes a routine restart look like the metric
/// was removed.
#[cfg(with_metrics)]
pub fn init_metrics() {
    linera_base::init_metrics();
    value_cache::metrics::init_metrics();
}

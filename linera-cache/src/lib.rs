// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Caching utilities for the Linera protocol.

mod unique_value_cache;
mod value_cache;

pub use unique_value_cache::UniqueValueCache;
pub use value_cache::ValueCache;

#[cfg(with_metrics)]
pub(crate) mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{register_int_counter_vec, register_int_gauge_vec};
    use prometheus::{IntCounterVec, IntGaugeVec};

    pub static CACHE_HIT: LazyLock<IntCounterVec> =
        LazyLock::new(|| register_int_counter_vec("cache_hit", "Cache hits", &["name", "type"]));

    pub static CACHE_MISS: LazyLock<IntCounterVec> =
        LazyLock::new(|| register_int_counter_vec("cache_miss", "Cache misses", &["name", "type"]));

    pub static CACHE_ENTRIES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec("cache_entries", "Current entry count", &["name", "type"])
    });

    pub static CACHE_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec(
            "cache_bytes",
            "Total weight (bytes) in cache",
            &["name", "type"],
        )
    });

    pub static CACHE_INVALIDATION: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "cache_invalidation",
            "Full-cache invalidation count",
            &["name", "type"],
        )
    });

    pub static CACHE_EVICTION: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "cache_eviction",
            "Entries evicted from cache",
            &["name", "type"],
        )
    });
}

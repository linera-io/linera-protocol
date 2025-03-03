// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::LazyLock;

// Re-export for macros.
#[doc(hidden)]
pub use linera_base::prometheus_util::{self, exponential_bucket_latencies};
use prometheus::IntCounterVec;

/// Increments the metrics counter with the given name, with the struct and base key as labels.
pub fn increment_counter(counter: &LazyLock<IntCounterVec>, struct_name: &str, base_key: &[u8]) {
    let base_key = hex::encode(base_key);
    let labels = [struct_name, &base_key];
    counter.with_label_values(&labels).inc();
}

/// The metric tracking the latency of the loading of views.
#[doc(hidden)]
pub static LOAD_VIEW_LATENCY: LazyLock<prometheus::HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "load_view_latency",
        "Load view latency",
        &[],
        exponential_bucket_latencies(10.0),
    )
});

/// The metric counting how often a view is read from storage.
#[doc(hidden)]
pub static LOAD_VIEW_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus_util::register_int_counter_vec(
        "load_view",
        "The metric counting how often a view is read from storage",
        &["type", "base_key"],
    )
});
/// The metric counting how often a view is written from storage.
#[doc(hidden)]
pub static SAVE_VIEW_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus_util::register_int_counter_vec(
        "save_view",
        "The metric counting how often a view is written from storage",
        &["type", "base_key"],
    )
});

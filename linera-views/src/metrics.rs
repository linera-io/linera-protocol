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

linera_base::declare_metrics! {
    /// The metric tracking the latency of the loading of views.
    #[doc(hidden)]
    pub static LOAD_VIEW_LATENCY: prometheus::HistogramVec =
        prometheus_util::register_histogram_vec(
            "load_view_latency",
            "Load view latency in milliseconds",
            &[],
            exponential_bucket_latencies(1000.0),
        );

    /// The metric counting how often a view is read from storage.
    #[doc(hidden)]
    pub static LOAD_VIEW_COUNTER: IntCounterVec =
        prometheus_util::register_int_counter_vec(
            "load_view",
            "The metric counting how often a view is read from storage",
            &["type", "base_key"],
        );

    /// The metric counting how often a view is written from storage.
    #[doc(hidden)]
    pub static SAVE_VIEW_COUNTER: IntCounterVec =
        prometheus_util::register_int_counter_vec(
            "save_view",
            "The metric counting how often a view is written from storage",
            &["type", "base_key"],
        );

    /// The metric tracking the latency of saving views.
    #[doc(hidden)]
    pub static SAVE_VIEW_LATENCY: prometheus::HistogramVec =
        prometheus_util::register_histogram_vec(
            "save_view_latency",
            "Save view latency in milliseconds",
            &["type"],
            exponential_bucket_latencies(1000.0),
        );
}

#[cfg(test)]
mod tests {
    #[test]
    fn label_free_metrics_are_exported_before_their_code_path_runs() {
        crate::init_metrics();

        let families = prometheus::gather();
        let contains_keys = families
            .iter()
            .find(|family| family.get_name() == "linera_key_value_store_view_contains_keys_latency")
            .expect("a label-free metric must be exported once init_metrics has run");

        let histogram = contains_keys
            .get_metric()
            .first()
            .expect("registering the vector must also create its single label-free child")
            .get_histogram();
        assert_eq!(histogram.get_sample_count(), 0);
    }

    /// Registration alone exports nothing — a vector emits one series per child, and a
    /// labelled one has no children until a label combination is observed. This is why
    /// forcing the `LazyLock` is not on its own enough, and why labelled metrics stay
    /// workload-gated.
    #[test]
    fn labelled_metrics_stay_absent_until_a_label_combination_is_observed() {
        crate::init_metrics();

        let exported = prometheus::gather().iter().any(|family| {
            family.get_name() == "linera_load_view" && !family.get_metric().is_empty()
        });
        assert!(!exported);
    }
}

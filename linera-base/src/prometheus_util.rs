// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines util functions for interacting with Prometheus (logging metrics, etc)

use prometheus::{
    histogram_opts, register_histogram_vec, register_int_counter_vec, Error, HistogramVec,
    IntCounterVec, Opts,
};

const LINERA_NAMESPACE: &str = "linera";

/// Wrapper arount prometheus register_int_counter_vec! macro which also sets the linera namespace
pub fn register_int_counter_vec(
    name: &str,
    description: &str,
    label_names: &[&str],
) -> Result<IntCounterVec, Error> {
    let counter_opts = Opts::new(name, description).namespace(LINERA_NAMESPACE);
    register_int_counter_vec!(counter_opts, label_names)
}

/// Wrapper arount prometheus register_histogram_vec! macro which also sets the linera namespace
pub fn register_histogram_vec(
    name: &str,
    description: &str,
    label_names: &[&str],
    buckets: Option<Vec<f64>>,
) -> Result<HistogramVec, Error> {
    let histogram_opts = if let Some(buckets) = buckets {
        histogram_opts!(name, description, buckets).namespace(LINERA_NAMESPACE)
    } else {
        histogram_opts!(name, description).namespace(LINERA_NAMESPACE)
    };

    register_histogram_vec!(histogram_opts, label_names)
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::LazyLock;

use linera_base::prometheus_util::{self};
use prometheus::{Histogram, HistogramVec, IntCounterVec, IntGaugeVec};

pub(crate) static GET_BLOB_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    prometheus_util::register_histogram(
        "get_blob_histogram_ms",
        "Time it took to read a blob from the storage",
        None,
    )
});

pub(crate) static GET_CERTIFICATE_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    prometheus_util::register_histogram(
        "get_certificate_histogram",
        "Time it took to read a certificate from the storage",
        None,
    )
});

pub(crate) static GET_CANONICAL_BLOCK_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    prometheus_util::register_histogram(
        "get_canonical_block_histogram_ms",
        "Time it took to read a canonical block from the storage",
        None,
    )
});

pub(crate) static SAVE_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    prometheus_util::register_histogram(
        "block_processor_state_save_histogram_ms",
        "Time it took to save the exporter state to the storage",
        None,
    )
});

pub(crate) static DISPATCH_BLOCK_HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "dispatch_block_histogram_ms",
        "Time it took to dispatch a block to a destination",
        &["destination"],
        None,
    )
});

pub(crate) static DISPATCH_BLOB_HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "dispatch_blob_histogram_ms",
        "Time it took to dispatch a blob to a validator destination",
        &["destination"],
        None,
    )
});

pub(crate) static DESTINATION_STATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus_util::register_int_counter_vec(
        "destination_state_counter",
        "Current state (height) of the destination as seen by the exporter",
        &["destination"],
    )
});

pub(crate) static VALIDATOR_EXPORTER_QUEUE_LENGTH: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    prometheus_util::register_int_gauge_vec(
        "validator_exporter_queue_length",
        "Length of the block queue for validator exporters",
        &["destination"],
    )
});

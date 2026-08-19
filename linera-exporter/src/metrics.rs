// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::prometheus_util::{self};
use prometheus::{Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec};

linera_base::declare_metrics! {
    pub(crate) static GET_BLOB_HISTOGRAM: Histogram =
        prometheus_util::register_histogram_with_subsystem(
            "exporter",
            "get_blob_ms",
            "Time it took to read a blob from the storage",
            None,
        );

    pub(crate) static GET_CERTIFICATE_HISTOGRAM: Histogram =
        prometheus_util::register_histogram_with_subsystem(
            "exporter",
            "get_certificate_ms",
            "Time it took to read a certificate from the storage",
            None,
        );

    pub(crate) static GET_CANONICAL_BLOCK_HISTOGRAM: Histogram =
        prometheus_util::register_histogram_with_subsystem(
            "exporter",
            "get_canonical_block_ms",
            "Time it took to read a canonical block from the storage",
            None,
        );

    pub(crate) static SAVE_HISTOGRAM: Histogram =
        prometheus_util::register_histogram_with_subsystem(
            "exporter",
            "state_save_ms",
            "Time it took to save the exporter state to the storage",
            None,
        );

    pub(crate) static DISPATCH_BLOCK_HISTOGRAM: HistogramVec =
        prometheus_util::register_histogram_vec_with_subsystem(
            "exporter",
            "dispatch_block_ms",
            "Time it took to dispatch a block to a destination",
            &["destination"],
            None,
        );

    pub(crate) static DISPATCH_BLOB_HISTOGRAM: HistogramVec =
        prometheus_util::register_histogram_vec_with_subsystem(
            "exporter",
            "dispatch_blob_ms",
            "Time it took to dispatch a blob to a validator destination",
            &["destination"],
            None,
        );

    pub(crate) static DESTINATION_STATE_COUNTER: IntCounterVec =
        prometheus_util::register_int_counter_vec_with_subsystem(
            "exporter",
            "destination_state_counter",
            "Current state (height) of the destination as seen by the exporter",
            &["destination"],
        );

    pub(crate) static VALIDATOR_EXPORTER_QUEUE_LENGTH: IntGaugeVec =
        prometheus_util::register_int_gauge_vec_with_subsystem(
            "exporter",
            "validator_queue_length",
            "Length of the block queue for validator exporters",
            &["destination"],
        );

    pub(crate) static EXPORTER_NOTIFICATION_QUEUE_LENGTH: IntGauge =
        prometheus_util::register_int_gauge_with_subsystem(
            "exporter",
            "notification_queue_length",
            "Length of the notification queue for the exporter service",
        );

    pub(crate) static NOTIFICATIONS_RECEIVED: IntCounter =
        prometheus_util::register_int_counter_with_subsystem(
            "exporter",
            "notifications_received",
            "Number of block notifications received from the notifier service",
        );

    pub(crate) static CANONICAL_STATE_HEIGHT: IntGauge =
        prometheus_util::register_int_gauge_with_subsystem(
            "exporter",
            "canonical_state_height",
            "Current height of the canonical state",
        );

    pub(crate) static KEEPALIVES_SENT: IntCounter =
        prometheus_util::register_int_counter_with_subsystem(
            "exporter",
            "keepalives_sent",
            "Number of keepalive messages sent to the indexer",
        );
}

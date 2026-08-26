// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Block exporter for the Linera protocol.

#![deny(missing_docs)]

/// Shared data types used across the exporter.
pub mod common;
pub mod config;
/// The gRPC service that serves exported blocks.
pub mod exporter_service;
/// Prometheus metrics for the exporter.
#[cfg(with_metrics)]
pub mod metrics;
/// The run loops that drive block export to each destination.
pub mod runloops;
/// The persistent state tracking export progress.
pub mod state;
/// Storage backends for the exporter.
pub mod storage;
/// Assorted helper utilities.
pub mod util;

#[cfg(test)]
mod test_utils;

/// Registers every metric this crate declares.
///
/// Without this, a metric is only exported after the code path that observes it has run, so a
/// rarely-taken path leaves its panels blank and makes a routine restart look like the metric
/// was removed.
#[cfg(with_metrics)]
pub fn init_metrics() {
    linera_base::init_metrics();
    linera_chain::init_metrics();
    linera_core::init_metrics();
    linera_execution::init_metrics();
    linera_rpc::init_metrics();
    linera_storage::init_metrics();
    linera_views::init_metrics();
    metrics::init_metrics();
}

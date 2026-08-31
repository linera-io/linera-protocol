// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the core Linera protocol.
//!
//! The consensus protocol it drives is specified and proved correct in the `linera-spec` crate,
//! which indexes both halves of the argument: safety and accountability in `linera_chain`, and
//! progress and liveness in [`proof`], next to the client code that discharges them.

#![recursion_limit = "256"]
#![deny(missing_docs)]
// We conditionally add autotraits to the traits here.
#![allow(async_fn_in_trait)]

mod chain_worker;
pub use chain_worker::{
    spawn_block_export_queue, BlockExportConfig, BlockExportHandle, ChainWorkerConfig,
    ProcessConfirmedBlockMode,
};
/// The high-level client for interacting with chains and validators.
pub mod client;
pub use client::Client;
/// Data types exchanged between clients, workers, and validator nodes.
pub mod data_types;
pub mod delegate;
pub mod join_set_ext;
mod local_node;
/// Traits for communicating with validator nodes.
pub mod node;
/// Utilities for notifying subscribers about chain events.
pub mod notifier;
pub mod proof;
/// A validator node paired with the validator's public key.
pub mod remote_node;
/// Helpers for writing tests against the core protocol.
#[cfg(with_testing)]
#[path = "unit_tests/test_utils.rs"]
pub mod test_utils;
/// The worker that validates and processes blocks and certificates for chains.
pub mod worker;

pub(crate) mod updater;

pub use local_node::LocalNodeError;
pub use updater::DEFAULT_QUORUM_GRACE_PERIOD;

pub use crate::join_set_ext::{JoinSetExt, TaskHandle};

/// The execution environment tying together storage, networking, signing, and the wallet.
pub mod environment;
/// The genesis configuration describing a network's initial chains and committee.
pub mod genesis_config;
pub use environment::{
    wallet::{self, Wallet},
    Environment,
};
pub use genesis_config::GenesisConfig;

/// The maximum number of entries in a `received_log` included in a `ChainInfo` response.
// TODO(#4638): Revisit the number.
pub const CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES: usize = 20_000;

/// Registers every metric this crate declares.
///
/// Without this, a metric is only exported after the code path that observes it has run, so a
/// rarely-taken path leaves its panels blank and makes a routine restart look like the metric
/// was removed.
#[cfg(with_metrics)]
pub fn init_metrics() {
    linera_base::init_metrics();
    linera_cache::init_metrics();
    linera_chain::init_metrics();
    linera_execution::init_metrics();
    linera_storage::init_metrics();
    linera_views::init_metrics();
    chain_worker::export::metrics::init_metrics();
    chain_worker::state::metrics::init_metrics();
    client::metrics::init_metrics();
    client::requests_scheduler::init_metrics();
    updater::metrics::init_metrics();
    worker::metrics::init_metrics();
}

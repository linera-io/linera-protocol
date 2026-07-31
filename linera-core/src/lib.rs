// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the core Linera protocol.

#![recursion_limit = "256"]
#![deny(missing_docs)]
// We conditionally add autotraits to the traits here.
#![allow(async_fn_in_trait)]

mod chain_worker;
pub use chain_worker::{ChainWorkerConfig, ProcessConfirmedBlockMode};
/// The high-level client for interacting with chains and validators.
pub mod client;
pub use client::Client;
/// Data types exchanged between clients, workers, and validator nodes.
pub mod data_types;
pub mod join_set_ext;
mod local_node;
/// Traits for communicating with validator nodes.
pub mod node;
/// Utilities for notifying subscribers about chain events.
pub mod notifier;
mod remote_node;
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

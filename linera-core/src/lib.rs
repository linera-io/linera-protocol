// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the core Linera protocol.

#![recursion_limit = "256"]
// We conditionally add autotraits to the traits here.
#![allow(async_fn_in_trait)]

mod chain_worker;
pub mod client;
pub use client::Client;
pub mod data_types;
mod genesis_config;
pub use genesis_config::{GenesisConfig, GenesisConfigError};
pub mod join_set_ext;
mod local_node;
pub mod node;
pub mod notifier;
mod remote_node;
#[cfg(with_testing)]
#[path = "unit_tests/test_utils.rs"]
pub mod test_utils;
pub mod worker;

pub(crate) mod updater;
mod value_cache;

pub use local_node::LocalNodeError;
pub use updater::DEFAULT_QUORUM_GRACE_PERIOD;

pub use crate::join_set_ext::{JoinSetExt, TaskHandle};

pub mod environment;
pub use environment::{
    wallet::{self, Wallet},
    Environment,
};

/// The maximum number of entries in a `received_log` included in a `ChainInfo` response.
// TODO(#4638): Revisit the number.
pub const CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES: usize = 20_000;

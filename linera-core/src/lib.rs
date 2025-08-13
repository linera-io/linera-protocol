// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the core Linera protocol.

#![recursion_limit = "256"]
#![deny(clippy::large_futures)]

mod chain_worker;
pub mod client;
pub mod data_types;
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
pub use updater::DEFAULT_GRACE_PERIOD;

pub use crate::join_set_ext::{JoinSetExt, TaskHandle};

pub mod environment;
pub use environment::Environment;

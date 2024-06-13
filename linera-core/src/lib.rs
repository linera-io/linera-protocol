// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the core Linera protocol.

pub mod chain_worker;
pub mod client;
pub mod data_types;
mod join_set_ext;
pub mod local_node;
pub mod node;
pub mod notifier;
#[cfg(with_testing)]
#[path = "unit_tests/test_utils.rs"]
pub mod test_utils;
pub mod worker;

pub(crate) mod updater;
pub(crate) mod value_cache;

pub use crate::join_set_ext::{JoinSetExt, TaskHandle};

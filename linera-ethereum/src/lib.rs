// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides functionalities for accessing an Ethereum blockchain node.

pub mod client;
pub mod common;

/// Helper types for tests and similar purposes.
#[cfg(not(target_arch = "wasm32"))]
pub mod test_utils;

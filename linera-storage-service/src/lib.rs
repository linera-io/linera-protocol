// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides a shared key-value store server based on the RocksDB store and the in-memory store of `linera-views`. It also includes the corresponding client and end-to-end tests.

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

#[cfg(any(test, feature = "test"))]
pub mod child;
pub mod client;
pub mod common;

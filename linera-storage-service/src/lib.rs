// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides a shared key-value store server based on the RocksDB store and the in-memory store of `linera-views`.
//! The corresponding client implements the `KeyValueStore` and `AdminKeyValueStore` traits.

#![deny(clippy::large_futures)]

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

pub mod child;
pub mod client;
pub mod common;

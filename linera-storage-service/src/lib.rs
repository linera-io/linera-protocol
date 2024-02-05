// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides the executables needed to provides a shared RocksDB/memory KeyValueStore server. It also provides the client functionality and tests them.

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

pub mod common;
pub mod client;

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Caching utilities for the Linera protocol.

mod unique_value_cache;
mod value_cache;

pub use unique_value_cache::UniqueValueCache;
pub use value_cache::{ValueCache, DEFAULT_CLEANUP_INTERVAL_SECS};

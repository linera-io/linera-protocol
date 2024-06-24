// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the linera-indexer library including:
//! - the indexer connection to the node service (service.rs)
//! - the block processing (indexer.rs)
//! - the generic plugin trait (plugin.rs)
//! - the runner struct (runner.rs)

pub mod common;
pub mod indexer;
pub mod plugin;
pub mod runner;
pub mod service;

#[cfg(feature = "rocksdb")]
pub mod rocks_db;
#[cfg(feature = "scylladb")]
pub mod scylla_db;
pub mod storage_service;

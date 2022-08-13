// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module aims to help the mapping of complex data-structures onto a key-value
//! store. The central notion is a [`views::View`] which can be loaded from storage, modified in
//! memory, then committed (i.e. the changes are atomically persisted in storage).

/// The main definitions.
pub mod views;

/// Helper definitions for in-memory storage.
pub mod memory;

/// Helper definitions for Rocksdb storage.
pub mod rocksdb;

/// Module to supporting hashing.
pub mod hash;

/// Macro definitions.
mod macros;

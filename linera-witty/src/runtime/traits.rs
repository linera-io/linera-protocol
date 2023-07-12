// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Abstractions over different Wasm runtime implementations.

/// A Wasm runtime.
///
/// Shared types between different guest instances that use the same runtime.
pub trait Runtime: Sized {}

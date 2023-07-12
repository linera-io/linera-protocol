// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common error type for usage of different Wasm runtimes.

use thiserror::Error;

/// Errors that can occur when using a Wasm runtime.
#[derive(Debug, Error)]
pub enum RuntimeError {}

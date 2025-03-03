// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides functionalities for accessing an Ethereum blockchain node.
//! Enabling the `ethereum` allows to make the tests work. This requires installing
//! the `anvil` from [FOUNDRY] and the [SOLC] compiler version 0.8.25
//!
//! [FOUNDRY]: https://book.getfoundry.sh/
//! [SOLC]: https://soliditylang.org/

pub mod client;
pub mod common;

#[cfg(not(target_arch = "wasm32"))]
pub mod provider;

/// Helper types for tests and similar purposes.
#[cfg(not(target_arch = "wasm32"))]
pub mod test_utils;

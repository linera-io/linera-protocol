// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides functionalities for accessing an Ethereum blockchain node.
//! Anabling the `ethereum` allows to make the test works. This requires installing
//! the `anvil` from [FOUNDRY] and the [SOLC] compiler version 0.8.25
//!
//! [FOUNDRY]: https://book.getfoundry.sh/
//! [SOLC]: https://soliditylang.org/

pub mod client;
pub mod common;

// /// Helper types for tests and similar purposes.
//pub mod test_utils;

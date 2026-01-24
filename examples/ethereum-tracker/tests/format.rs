// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the ethereum-tracker application.

#![cfg(not(target_arch = "wasm32"))]

use ethereum_tracker::formats::EthereumTrackerApplication;
use linera_sdk::formats::BcsApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", EthereumTrackerApplication::formats().unwrap());
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the call-evm-counter application.

#![cfg(not(target_arch = "wasm32"))]

use call_evm_counter::formats::CallEvmCounterApplication;
use linera_sdk::abis::formats::BcsApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", CallEvmCounterApplication::formats().unwrap());
}

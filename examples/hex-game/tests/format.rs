// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the hex-game application.

#![cfg(not(target_arch = "wasm32"))]

use hex_game::formats::HexApplication;
use linera_sdk::abis::formats::BcsApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", HexApplication::formats().unwrap());
}

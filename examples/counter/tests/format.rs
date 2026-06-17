// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the counter application.

#![cfg(not(target_arch = "wasm32"))]

use counter::formats::CounterApplication;
use linera_sdk::formats::BcsApplication;

#[test]
fn test_format() {
    let mut formats = CounterApplication::formats().unwrap();
    formats.prune_known_primitives().unwrap();
    insta::assert_yaml_snapshot!("format", formats);
}

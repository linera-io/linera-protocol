// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the counter-no-graphql application.

#![cfg(not(target_arch = "wasm32"))]

use counter_no_graphql::formats::CounterApplication;
use linera_sdk::abis::formats::BcsApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", CounterApplication::formats().unwrap());
}

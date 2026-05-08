// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the matching-engine application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::formats::BcsApplication;
use matching_engine::formats::MatchingEngineApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", MatchingEngineApplication::formats().unwrap());
}

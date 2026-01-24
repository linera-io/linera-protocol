// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the crowd-funding application.

#![cfg(not(target_arch = "wasm32"))]

use crowd_funding::formats::CrowdFundingApplication;
use linera_sdk::formats::BcsApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", CrowdFundingApplication::formats().unwrap());
}

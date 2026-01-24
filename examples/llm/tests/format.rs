// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the llm application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::abis::formats::BcsApplication;
use llm::formats::LlmApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", LlmApplication::formats().unwrap());
}

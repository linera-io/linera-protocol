// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the wrapped-fungible application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::formats::BcsApplication;
use wrapped_fungible::formats::WrappedFungibleApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", WrappedFungibleApplication::formats().unwrap());
}

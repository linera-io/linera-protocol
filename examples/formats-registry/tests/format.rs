// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the formats-registry application.

#![cfg(not(target_arch = "wasm32"))]

use formats_registry::formats::FormatsRegistryApplication;
use linera_sdk::formats::BcsApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", FormatsRegistryApplication::formats().unwrap());
}

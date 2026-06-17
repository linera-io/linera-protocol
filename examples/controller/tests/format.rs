// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the controller application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::{abis::controller::formats::ControllerApplication, formats::BcsApplication};

#[test]
fn test_format() {
    let mut formats = ControllerApplication::formats().unwrap();
    formats.prune_known_primitives().unwrap();
    insta::assert_yaml_snapshot!("format", formats);
}

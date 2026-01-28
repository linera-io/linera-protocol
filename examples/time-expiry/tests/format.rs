// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the time-expiry application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::formats::BcsApplication;
use time_expiry::formats::TimeExpiryApplication;

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", TimeExpiryApplication::formats().unwrap());
}

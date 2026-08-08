// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::util::parse_app_set;

#[test]
fn parse_app_set_accepts_empty() {
    assert!(parse_app_set("").unwrap().is_empty());
    assert!(parse_app_set("   ").unwrap().is_empty());
    assert!(parse_app_set("not-an-application-id").is_err());
}

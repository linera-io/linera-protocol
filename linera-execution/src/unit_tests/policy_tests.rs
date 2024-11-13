// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `policy` module types.

use super::ResourceLimit;

/// Check that the default [`ResourceLimit`] is the maximal value.
#[test]
fn default_resource_limit() {
    assert_eq!(ResourceLimit::default(), ResourceLimit(u64::MAX));
}

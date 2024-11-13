// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `policy` module types.

use super::ResourceLimit;

/// Check that the default [`ResourceLimit`] is the maximal value.
#[test]
fn default_resource_limit() {
    assert_eq!(ResourceLimit::default(), ResourceLimit(u64::MAX));
}

/// Test the JSON serialization round-trip of the [`ResourceLimit`].
///
/// Checks if a large value is serialized as a string, instead of being serialized as a
/// floating-point number which would lose precision.
#[test]
fn resource_limit_json_roundtrip() -> anyhow::Result<()> {
    let initial_value = ResourceLimit::default();
    let json = serde_json::to_string(&initial_value)?;
    let recovered_value = serde_json::from_str::<ResourceLimit>(&json)?;

    assert_eq!(json, format!("\"{}\"", u64::MAX));
    assert_eq!(recovered_value, initial_value);

    Ok(())
}

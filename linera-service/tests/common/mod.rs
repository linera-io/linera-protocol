// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::env;

/// Restores the `RUSTFLAGS` environment variable to make warnings fail as errors.
pub struct RestoreVarOnDrop;

impl Drop for RestoreVarOnDrop {
    fn drop(&mut self) {
        env::set_var("RUSTFLAGS", "-D warnings");
    }
}

/// Clears the `RUSTFLAGS` environment variable, if it was configured to make warnings fail as
/// errors.
///
/// The returned [`RestoreVarOnDrop`] restores the environment variable to its original value when
/// it is dropped.
pub fn override_disable_warnings_as_errors() -> Option<RestoreVarOnDrop> {
    if matches!(env::var("RUSTFLAGS"), Ok(value) if value == "-D warnings") {
        env::set_var("RUSTFLAGS", "");
        Some(RestoreVarOnDrop)
    } else {
        None
    }
}

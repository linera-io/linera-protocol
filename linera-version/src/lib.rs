// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!

This crate is in charge of extracting version information from the Linera build, for
troubleshooting information and version compatibility checks.

*/

mod serde_pretty;
pub use serde_pretty::*;

mod version_info;
pub use version_info::*;

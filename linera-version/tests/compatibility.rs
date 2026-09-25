// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_version::{CrateVersion, Pretty, VersionInfo};

/// Builds a version whose three API hashes are all `hash`, all unknown if it is `None`.
fn version_info(patch: u32, hash: Option<&'static str>) -> VersionInfo {
    VersionInfo {
        crate_version: Pretty::new(CrateVersion {
            major: 0,
            minor: 15,
            patch,
        }),
        git_commit: "0000000000".into(),
        git_dirty: false,
        rpc_hash: hash.map(Into::into),
        graphql_hash: hash.map(Into::into),
        wit_hash: hash.map(Into::into),
    }
}

/// A remote ahead by a patch release fails the version check, so its hashes decide.
#[test]
fn identical_hashes_bridge_a_patch_gap() {
    assert!(version_info(23, Some("hash")).is_compatible_with(&version_info(20, Some("hash"))));
    assert!(!version_info(23, Some("other")).is_compatible_with(&version_info(20, Some("hash"))));
}

#[test]
fn unknown_hashes_never_establish_compatibility() {
    assert!(!version_info(23, None).is_compatible_with(&version_info(20, None)));
    assert!(!version_info(23, None).is_compatible_with(&version_info(20, Some("hash"))));
}

/// Unknown hashes leave the version check untouched.
#[test]
fn unknown_hashes_still_allow_a_compatible_version() {
    assert!(version_info(20, None).is_compatible_with(&version_info(23, None)));
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_version::VersionInfo;

#[test]
fn up_to_date() {
    assert_eq!(VersionInfo::get().unwrap(), VersionInfo::default(),);
}

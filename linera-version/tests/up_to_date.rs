// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_version::VersionInfo;

#[test]
fn up_to_date() {
    assert_eq!(
        VersionInfo::get().unwrap().api_hashes(),
        VersionInfo::default().api_hashes(),
        "`linera-version` API hash cache out of date.\n\
         Please update `linera-version/api-hashes.json` by running:\n\
         $ cargo run linera-version > linera-version/api-hashes.json"
    );
}

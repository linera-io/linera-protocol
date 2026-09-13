// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_version::{ApiHashes, VersionInfo};

/// Checks the committed hash cache against the hashes computed from the API source files.
///
/// The cache is only read when those files cannot be located, which is the case for a
/// published crate but not within this repository.
#[test]
fn up_to_date() {
    let cached_path = concat!(env!("CARGO_MANIFEST_DIR"), "/api-hashes.json");
    let cached: ApiHashes = serde_json::from_str(
        &std::fs::read_to_string(cached_path)
            .unwrap_or_else(|error| panic!("failed to read `{cached_path}`: {error}")),
    )
    .unwrap_or_else(|error| panic!("failed to parse `{cached_path}`: {error}"));

    assert_eq!(
        VersionInfo::get().unwrap().api_hashes(),
        cached,
        "`linera-version` API hash cache out of date.\n\
         Please update `linera-version/api-hashes.json` by running:\n\
         $ cargo run -p linera-version > linera-version/api-hashes.json"
    );
}

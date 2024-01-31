// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

include!("src/version_info.rs");

fn main() {
    let mut paths = vec![];
    let version_info = VersionInfo::trace_get(&mut paths).unwrap();

    for path in paths {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    println!("cargo:rustc-cfg=linera_version_building");

    println!(
        "cargo:rustc-env=LINERA_VERSION_CRATE_VERSION={}",
        version_info.crate_version
    );
    println!(
        "cargo:rustc-env=LINERA_VERSION_GIT_COMMIT={}",
        version_info.git_commit
    );
    if version_info.git_dirty {
        println!("cargo:rustc-cfg=linera_version_git_dirty");
    }
    println!(
        "cargo:rustc-env=LINERA_VERSION_RPC_HASH={}",
        version_info.rpc_hash
    );
    println!(
        "cargo:rustc-env=LINERA_VERSION_GRAPHQL_HASH={}",
        version_info.graphql_hash
    );
    println!(
        "cargo:rustc-env=LINERA_VERSION_WIT_HASH={}",
        version_info.wit_hash
    );
}

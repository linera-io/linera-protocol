// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO(#2239): these tests fail to build
#![cfg(any())]

use std::path::Path;

use linera_base::util;

#[test_log::test(tokio::test)]
async fn test_resolve_binary_with_test_default() {
    let path = util::resolve_binary("linera", "linera-service")
        .await
        .unwrap();
    assert!(path.exists());
    // Since we're in a test, we can use the environment variables `CARGO_BIN_EXE_*`.
    assert_eq!(path, Path::new(env!("CARGO_BIN_EXE_linera")));
}

#[test_log::test(tokio::test)]
async fn test_resolve_binary_from_relative_path() {
    let debug_or_release = Path::new(env!("CARGO_BIN_EXE_linera"))
        .parent()
        .unwrap()
        .file_name()
        .unwrap();
    let path = util::resolve_binary_in_same_directory_as(
        Path::new("../target").join(debug_or_release).join("linera"),
        "linera-proxy",
        "linera-service",
    )
    .await
    .unwrap();
    assert!(path.exists());
    assert_eq!(path, Path::new(env!("CARGO_BIN_EXE_linera-proxy")));
}

#[test_log::test(tokio::test)]
async fn test_resolve_binary_from_absolute_path() {
    let path = util::resolve_binary_in_same_directory_as(
        env!("CARGO_BIN_EXE_linera"),
        "linera-proxy",
        "linera-service",
    )
    .await
    .unwrap();
    assert!(path.exists());
    assert_eq!(path, Path::new(env!("CARGO_BIN_EXE_linera-proxy")));
}

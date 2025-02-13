// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(test)]  // Enables these tests only in test mode

use std::path::Path;
use std::fs;
use std::env;

use linera_base::util;

#[test_log::test(tokio::test)]
async fn test_resolve_binary_with_test_default() {
    let path = util::resolve_binary("linera", "linera-service")
        .await
        .expect("Failed to resolve linera binary");

    assert!(
        fs::metadata(&path).is_ok(),
        "Binary path does not exist: {:?}",
        path
    );
    assert_eq!(path, Path::new(env!("CARGO_BIN_EXE_linera")));
}

#[test_log::test(tokio::test)]
async fn test_resolve_binary_from_relative_path() {
    let debug_or_release = Path::new(env!("CARGO_BIN_EXE_linera"))
        .parent()
        .expect("Failed to get parent directory")
        .file_name()
        .expect("Failed to get directory name");

    let path = util::resolve_binary_in_same_directory_as(
        Path::new("../target").join(debug_or_release).join("linera"),
        "linera-proxy",
        "linera-service",
    )
    .await;

    assert!(path.is_ok(), "Failed to resolve binary from relative path");
    let path = path.unwrap();
    assert!(
        fs::metadata(&path).is_ok(),
        "Binary path does not exist: {:?}",
        path
    );
    assert_eq!(path, Path::new(env!("CARGO_BIN_EXE_linera-proxy")));
}

#[test_log::test(tokio::test)]
async fn test_resolve_binary_from_absolute_path() {
    let path = util::resolve_binary_in_same_directory_as(
        env!("CARGO_BIN_EXE_linera"),
        "linera-proxy",
        "linera-service",
    )
    .await;

    assert!(path.is_ok(), "Failed to resolve binary from absolute path");
    let path = path.unwrap();
    assert!(
        fs::metadata(&path).is_ok(),
        "Binary path does not exist: {:?}",
        path
    );
    assert_eq!(path, Path::new(env!("CARGO_BIN_EXE_linera-proxy")));
}

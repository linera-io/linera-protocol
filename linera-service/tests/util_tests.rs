// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "rocksdb")]

use linera_service::util;
use std::path::Path;

#[test_log::test(tokio::test)]
async fn test_resolve_binary_with_test_default() {
    util::resolve_binary("linera", "linera-service")
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_resolve_binary_from_relative_path() {
    util::resolve_binary_in_same_directory_as(
        "../target/debug/linera",
        "linera-proxy",
        "linera-service",
    )
    .await
    .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_resolve_binary_from_absolute_path() {
    let path = Path::new("../target/debug/linera").canonicalize().unwrap();
    util::resolve_binary_in_same_directory_as(&path, "linera-proxy", "linera-service")
        .await
        .unwrap();
}

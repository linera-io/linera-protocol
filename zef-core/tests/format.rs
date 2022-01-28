// Copyright (c) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0

#[test]
fn test_format() {
    let status = std::process::Command::new("cargo")
        .current_dir("..")
        .args(&["run", "--example", "generate-format", "--"])
        .arg("test")
        .status()
        .expect("failed to execute process");
    assert!(status.success());
}

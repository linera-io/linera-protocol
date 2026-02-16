// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use serde_generate::{solidity, CodeGeneratorConfig, SourceInstaller};
use serde_reflection::Registry;

fn main() {
    let snap_path = PathBuf::from("tests/snapshots/format__format.yaml.snap");
    println!("cargo:rerun-if-changed={}", snap_path.display());

    if !snap_path.exists() {
        return;
    }

    let content = std::fs::read_to_string(&snap_path).expect("failed to read snapshot file");

    // Strip the insta snapshot header (everything up to and including the second "---" line).
    let yaml = content
        .splitn(3, "---")
        .nth(2)
        .expect("snapshot file missing insta header");

    let registry: Registry =
        serde_yaml::from_str(yaml).expect("failed to parse YAML registry from snapshot");

    let out_dir = PathBuf::from("src");
    let installer = solidity::Installer::new(out_dir);
    let config = CodeGeneratorConfig::new("BridgeTypes".to_string());
    installer
        .install_module(&config, &registry)
        .expect("failed to generate Solidity code");
}

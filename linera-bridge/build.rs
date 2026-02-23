// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use serde_generate::{solidity, CodeGeneratorConfig, SourceInstaller};
use serde_reflection::Registry;

fn main() {
    generate_bridge_types();
}

/// Generates BridgeTypes.sol from the bridge snapshot.
fn generate_bridge_types() {
    let snap_path = PathBuf::from("tests/snapshots/format__format.yaml.snap");
    let Some(registry) = read_snapshot_registry(&snap_path) else {
        return;
    };

    let out_dir = PathBuf::from("src/solidity");
    let installer = solidity::Installer::new(out_dir);
    let config = CodeGeneratorConfig::new("BridgeTypes".to_string());
    installer
        .install_module(&config, &registry)
        .expect("failed to generate Solidity code");
}

/// Reads an insta snapshot file and extracts the YAML registry from it.
fn read_snapshot_registry(snap_path: &PathBuf) -> Option<Registry> {
    println!("cargo:rerun-if-changed={}", snap_path.display());

    if !snap_path.exists() {
        return None;
    }

    let content = std::fs::read_to_string(snap_path).expect("failed to read snapshot file");

    // Strip the insta snapshot header (everything up to and including the second "---" line).
    let yaml = content
        .splitn(3, "---")
        .nth(2)
        .expect("snapshot file missing insta header");

    let registry: Registry =
        serde_yaml::from_str(yaml).expect("failed to parse YAML registry from snapshot");
    Some(registry)
}

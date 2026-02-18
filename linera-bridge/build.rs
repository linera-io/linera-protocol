// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, path::PathBuf};

use serde_generate::{solidity, CodeGeneratorConfig, SourceInstaller};
use serde_reflection::Registry;

fn main() {
    generate_bridge_types();
    generate_fungible_types();
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

/// Generates FungibleTypes.sol from the fungible snapshot.
/// Types shared with BridgeTypes are declared as external so the generated
/// code imports them from BridgeTypes.sol instead of redefining them.
fn generate_fungible_types() {
    let bridge_snap = PathBuf::from("tests/snapshots/format__format.yaml.snap");
    let fungible_snap = PathBuf::from("tests/snapshots/format_fungible__format_fungible.yaml.snap");

    let Some(bridge_registry) = read_snapshot_registry(&bridge_snap) else {
        return;
    };
    let Some(fungible_registry) = read_snapshot_registry(&fungible_snap) else {
        return;
    };

    // Types that exist in both registries are external (from BridgeTypes).
    let shared_types: Vec<String> = fungible_registry
        .keys()
        .filter(|name| bridge_registry.contains_key(*name))
        .cloned()
        .collect();

    let out_dir = PathBuf::from("src/solidity");
    let installer = solidity::Installer::new(out_dir);
    let config = CodeGeneratorConfig::new("FungibleTypes".to_string())
        .with_external_definitions(BTreeMap::from([("BridgeTypes".to_string(), shared_types)]));
    installer
        .install_module(&config, &fungible_registry)
        .expect("failed to generate FungibleTypes Solidity code");
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

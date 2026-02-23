// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    #[cfg(feature = "codegen")]
    codegen::generate();
}

#[cfg(feature = "codegen")]
mod codegen {
    use std::{collections::BTreeMap, path::PathBuf};

    use serde_generate::{solidity, CodeGeneratorConfig, SourceInstaller};
    use serde_reflection::Registry;

    pub fn generate() {
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
    /// Primitive types shared with BridgeTypes are declared as external so the generated
    /// code imports them from BridgeTypes.sol instead of redefining them.
    fn generate_fungible_types() {
        let bridge_snap = PathBuf::from("tests/snapshots/format__format.yaml.snap");
        let fungible_snap =
            PathBuf::from("tests/snapshots/format_fungible__format_fungible.yaml.snap");

        let Some(bridge_registry) = read_snapshot_registry(&bridge_snap) else {
            return;
        };
        let Some(fungible_registry) = read_snapshot_registry(&fungible_snap) else {
            return;
        };

        let shared_types = bridge_type_names(&fungible_registry, &bridge_registry);

        let out_dir = PathBuf::from("src/solidity");
        let installer = solidity::Installer::new(out_dir);
        let config = CodeGeneratorConfig::new("FungibleTypes".to_string())
            .with_external_definitions(BTreeMap::from([("BridgeTypes".to_string(), shared_types)]));
        installer
            .install_module(&config, &fungible_registry)
            .expect("failed to generate FungibleTypes Solidity code");
    }

    /// Returns the names from `fungible_registry` that are primitive/structural types also
    /// present in `bridge_registry`. These are declared as external imports from BridgeTypes.sol.
    ///
    /// We can't simply use all names that appear in both registries because serde-reflection
    /// uses short type names (no module path), so unrelated types with the same name (e.g.
    /// `linera_execution::Message` vs `fungible::Message`) would collide.
    fn bridge_type_names(fungible_registry: &Registry, bridge_registry: &Registry) -> Vec<String> {
        // Primitive/structural types shared by both registries. These are the leaf types that
        // the fungible application's Operation and Message types are built from.
        const SHARED: &[&str] = &["Account", "AccountOwner", "Amount", "ChainId", "CryptoHash"];

        SHARED
            .iter()
            .filter(|name| {
                fungible_registry.contains_key(**name) && bridge_registry.contains_key(**name)
            })
            .map(|name| name.to_string())
            .collect()
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
}

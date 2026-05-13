// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    #[cfg(feature = "codegen")]
    codegen::generate();
}

#[cfg(feature = "codegen")]
mod codegen {
    use std::{collections::BTreeMap, path::PathBuf, process::Command};

    use serde_generate::{solidity, CodeGeneratorConfig, SourceInstaller};
    use serde_reflection::Registry;

    pub fn generate() {
        generate_bridge_types();
        generate_fungible_types();
        // The bytes20/bytes32 helpers in the generated output use plain `assembly { ... }`
        // for a single mload. Mark them memory-safe so the IR optimizer can spill freely;
        // without this LightClient.sol hits "stack too deep" when the deserializer tree
        // is inlined into verifyCertificate.
        mark_assembly_memory_safe(&PathBuf::from("src/solidity/BridgeTypes.sol"));
        forge_fmt(&PathBuf::from("src/solidity/BridgeTypes.sol"));
        forge_fmt(&PathBuf::from("src/solidity/WrappedFungibleTypes.sol"));
    }

    /// Rewrites `assembly { ... }` to `assembly ("memory-safe") { ... }` in a generated
    /// Solidity file. Called after `serde-generate` produces the file.
    fn mark_assembly_memory_safe(path: &PathBuf) {
        if !path.exists() {
            return;
        }
        let content = std::fs::read_to_string(path).expect("failed to read generated Solidity");
        let patched = content.replace("assembly {", "assembly (\"memory-safe\") {");
        if patched != content {
            std::fs::write(path, patched).expect("failed to write patched Solidity");
        }
    }

    /// Reformats a freshly generated Solidity file with `forge fmt` so the
    /// generator's output matches the rest of the codebase. Falls back to
    /// a warning (non-fatal) if `forge` is not on PATH — codegen is a
    /// best-effort developer convenience and shouldn't break a build that
    /// otherwise wouldn't have run forge.
    fn forge_fmt(path: &PathBuf) {
        if !path.exists() {
            return;
        }
        let status = Command::new("forge").arg("fmt").arg(path).status();
        match status {
            Ok(s) if s.success() => {}
            Ok(s) => {
                println!("cargo:warning=forge fmt {} exited with {s}", path.display());
            }
            Err(e) => {
                println!(
                    "cargo:warning=forge fmt {} could not be invoked: {e}",
                    path.display()
                );
            }
        }
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

    /// Generates WrappedFungibleTypes.sol from the wrapped-fungible snapshot.
    /// Primitive types shared with BridgeTypes are declared as external so the generated
    /// code imports them from BridgeTypes.sol instead of redefining them.
    fn generate_fungible_types() {
        let bridge_snap = PathBuf::from("tests/snapshots/format__format.yaml.snap");
        let fungible_snap = PathBuf::from(
            "tests/snapshots/format_wrapped_fungible__format_wrapped_fungible.yaml.snap",
        );

        let Some(bridge_registry) = read_snapshot_registry(&bridge_snap) else {
            return;
        };
        let Some(fungible_registry) = read_snapshot_registry(&fungible_snap) else {
            return;
        };

        let shared_types = bridge_type_names(&fungible_registry, &bridge_registry);

        let out_dir = PathBuf::from("src/solidity");
        let installer = solidity::Installer::new(out_dir);
        let config = CodeGeneratorConfig::new("WrappedFungibleTypes".to_string())
            .with_external_definitions(BTreeMap::from([("BridgeTypes".to_string(), shared_types)]));
        installer
            .install_module(&config, &fungible_registry)
            .expect("failed to generate WrappedFungibleTypes Solidity code");
    }

    /// Returns the names from `fungible_registry` that are primitive/structural types also
    /// present in `bridge_registry`. These are declared as external imports from BridgeTypes.sol.
    ///
    /// We can't simply use all names that appear in both registries because serde-reflection
    /// uses short type names (no module path), so unrelated types with the same name (e.g.
    /// `linera_execution::Message` vs `wrapped_fungible::Message`) would collide.
    fn bridge_type_names(fungible_registry: &Registry, bridge_registry: &Registry) -> Vec<String> {
        // Primitive/structural types shared by both registries. These are the leaf types that
        // the fungible application's Operation and Message types are built from.
        const SHARED: &[&str] = &["Account", "AccountOwner", "Amount", "ChainId", "CryptoHash"];

        SHARED
            .iter()
            .filter(|name| {
                fungible_registry.contains_key(**name) && bridge_registry.contains_key(**name)
            })
            .map(|name| (*name).to_string())
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

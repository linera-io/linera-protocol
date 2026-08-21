// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    #[cfg(feature = "codegen")]
    codegen::generate();
}

#[cfg(feature = "codegen")]
mod codegen {
    use std::{collections::BTreeMap, fs, path::PathBuf, process::Command};

    use serde_generate::{solidity, CodeGeneratorConfig, SourceInstaller};
    use serde_reflection::Registry;

    pub fn generate() {
        let generated = [
            PathBuf::from("src/solidity/BridgeTypes.sol"),
            PathBuf::from("src/solidity/WrappedFungibleTypesV1.sol"),
        ];
        // The generators overwrite the checked-in sources, and only `forge fmt` turns their
        // output into the committed form, so without it every build leaves the tree dirty
        // with equivalent-but-unformatted code that is easy to commit by accident.
        let committed: Vec<Option<String>> = generated
            .iter()
            .map(|path| fs::read_to_string(path).ok())
            .collect();

        generate_bridge_types();
        generate_fungible_types();

        for (path, committed) in generated.iter().zip(committed) {
            if forge_fmt(path) {
                continue;
            }
            if let Some(committed) = committed {
                if let Err(e) = fs::write(path, committed) {
                    println!("cargo:warning=could not restore {}: {e}", path.display());
                }
            }
        }
    }

    /// Reformats a freshly generated Solidity file with `forge fmt` so the generator's
    /// output matches the rest of the codebase, reporting whether it succeeded. Failure is
    /// non-fatal (codegen is a developer convenience and should not break a build that
    /// would not otherwise have run forge), but the caller restores the committed file,
    /// because unformatted output differs from it and would dirty the tree.
    fn forge_fmt(path: &PathBuf) -> bool {
        if !path.exists() {
            return false;
        }
        let status = Command::new("forge").arg("fmt").arg(path).status();
        match status {
            Ok(s) if s.success() => true,
            Ok(s) => {
                println!(
                    "cargo:warning=forge fmt {} exited with {s}; leaving the committed file \
                     in place",
                    path.display()
                );
                false
            }
            Err(e) => {
                println!(
                    "cargo:warning=forge fmt {} could not be invoked: {e}; leaving the \
                     committed file in place",
                    path.display()
                );
                false
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

    /// Generates WrappedFungibleTypesV1.sol from the wrapped-fungible snapshot.
    /// The `V1` suffix is versioned in lockstep with `FungibleBurnEventDecoderV1`:
    /// a future BurnEvent schema change generates a new `WrappedFungibleTypesV2`
    /// consumed by a new decoder. Primitive types shared with BridgeTypes are
    /// declared as external so the generated code imports them from
    /// BridgeTypes.sol instead of redefining them.
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
        let config = CodeGeneratorConfig::new("WrappedFungibleTypesV1".to_string())
            .with_external_definitions(BTreeMap::from([("BridgeTypes".to_string(), shared_types)]));
        installer
            .install_module(&config, &fungible_registry)
            .expect("failed to generate WrappedFungibleTypesV1 Solidity code");
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

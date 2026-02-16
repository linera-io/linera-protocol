// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, path::PathBuf};

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

/// Generates FungibleTypes.sol from the fungible snapshot, stripping types
/// that already exist in BridgeTypes.sol and importing them instead.
fn generate_fungible_types() {
    let bridge_snap = PathBuf::from("tests/snapshots/format__format.yaml.snap");
    let fungible_snap = PathBuf::from("tests/snapshots/format_fungible__format_fungible.yaml.snap");

    let Some(bridge_registry) = read_snapshot_registry(&bridge_snap) else {
        return;
    };
    let Some(fungible_registry) = read_snapshot_registry(&fungible_snap) else {
        return;
    };

    // Types unique to the fungible snapshot that don't appear in the bridge registry.
    let fungible_only_types: HashSet<String> = fungible_registry
        .keys()
        .filter(|name| !bridge_registry.contains_key(*name))
        .cloned()
        .collect();

    // Build a generation registry: fungible-only types from the snapshot, plus
    // complete definitions for shared types from the bridge registry.
    let mut generation_registry = fungible_registry;
    for (name, format) in &bridge_registry {
        if generation_registry.contains_key(name) {
            // Override incomplete definitions from registry_unchecked
            // with complete ones from the bridge registry.
            generation_registry.insert(name.clone(), format.clone());
        }
    }

    // Generate the FungibleTypes library to a temporary directory.
    let tmp_dir = PathBuf::from("src/tmp_fungible");
    std::fs::create_dir_all(&tmp_dir).expect("failed to create temp dir");

    let installer = solidity::Installer::new(tmp_dir.clone());
    let config = CodeGeneratorConfig::new("FungibleTypes".to_string());
    installer
        .install_module(&config, &generation_registry)
        .expect("failed to generate FungibleTypes Solidity code");

    let generated_path = tmp_dir.join("FungibleTypes.sol");
    let generated =
        std::fs::read_to_string(&generated_path).expect("failed to read generated FungibleTypes");

    // Post-process: keep only fungible-specific definitions, strip the rest.
    let processed = postprocess_fungible_types(&generated, &fungible_only_types);

    let final_path = PathBuf::from("src/solidity/FungibleTypes.sol");
    std::fs::write(&final_path, processed).expect("failed to write FungibleTypes.sol");

    // Clean up temporary directory.
    std::fs::remove_dir_all(&tmp_dir).ok();
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

/// Removes struct/enum/function definitions that already exist in BridgeTypes.sol,
/// and adds an import statement. Only definitions related to fungible-specific types
/// (those in the fungible-only snapshot) are kept.
fn postprocess_fungible_types(source: &str, fungible_only_types: &HashSet<String>) -> String {
    let lines: Vec<&str> = source.lines().collect();
    let mut result = Vec::new();
    let mut i = 0;
    let mut stripped_types: Vec<String> = Vec::new();
    let mut stripped_functions: Vec<String> = Vec::new();

    // Add import after the pragma line.
    let mut pragma_found = false;

    while i < lines.len() {
        let line = lines[i];

        // Insert import after pragma
        if !pragma_found && line.starts_with("pragma ") {
            result.push(line);
            result.push("");
            result.push("import \"BridgeTypes.sol\";");
            pragma_found = true;
            i += 1;
            continue;
        }

        // Check if this line starts a struct definition not in the fungible snapshot
        if let Some(type_name) = extract_struct_name(line) {
            if !is_fungible_type(&type_name, fungible_only_types) {
                stripped_types.push(type_name);
                i = skip_block(i, &lines);
                continue;
            }
        }

        // Check if this line starts an enum definition not in the fungible snapshot
        if let Some(type_name) = extract_enum_name(line) {
            if !is_fungible_type(&type_name, fungible_only_types) {
                stripped_types.push(type_name);
                i = skip_block(i, &lines);
                continue;
            }
        }

        // Check if this line starts a function not related to fungible types
        if let Some(fn_name) = extract_function_name(line) {
            if !is_fungible_function(&fn_name, fungible_only_types) {
                stripped_functions.push(fn_name);
                i = skip_block(i, &lines);
                continue;
            }
        }

        result.push(line);
        i += 1;
    }

    let mut output = result.join("\n");

    // Replace references to stripped types with BridgeTypes-qualified names.
    for type_name in &stripped_types {
        let qualified = format!("BridgeTypes.{}", type_name);
        output = replace_type_references(&output, type_name, &qualified);
    }

    // Replace calls to stripped functions with BridgeTypes-qualified calls.
    for fn_name in &stripped_functions {
        let qualified = format!("BridgeTypes.{}", fn_name);
        output = replace_type_references(&output, fn_name, &qualified);
    }

    // Clean up any double blank lines that may have been created by removing blocks.
    while output.contains("\n\n\n") {
        output = output.replace("\n\n\n", "\n\n");
    }

    output
}

/// Replaces unqualified type references with qualified ones.
/// Only replaces when the type name appears as a standalone type reference
/// (not as part of another identifier).
fn replace_type_references(source: &str, type_name: &str, qualified: &str) -> String {
    let mut result = String::with_capacity(source.len());
    let chars: Vec<char> = source.chars().collect();
    let type_chars: Vec<char> = type_name.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Check for potential match
        if i + type_chars.len() <= chars.len() && chars[i..i + type_chars.len()] == type_chars[..] {
            // Check that we're not already qualified (preceded by '.')
            let preceded_by_dot = i > 0 && chars[i - 1] == '.';
            // Check that this is not part of a larger identifier
            let preceded_by_ident =
                i > 0 && (chars[i - 1].is_alphanumeric() || chars[i - 1] == '_');
            let followed_by_ident = i + type_chars.len() < chars.len()
                && (chars[i + type_chars.len()].is_alphanumeric()
                    || chars[i + type_chars.len()] == '_');

            if !preceded_by_dot && !preceded_by_ident && !followed_by_ident {
                result.push_str(qualified);
                i += type_chars.len();
                continue;
            }
        }
        result.push(chars[i]);
        i += 1;
    }

    result
}

/// Checks if a type name belongs to the fungible-specific types — either exact match
/// or a sub-type (e.g. `FungibleOperation_Transfer` for `FungibleOperation`).
fn is_fungible_type(name: &str, fungible_types: &HashSet<String>) -> bool {
    if fungible_types.contains(name) {
        return true;
    }
    // Check if this is a sub-type of a fungible type
    fungible_types
        .iter()
        .any(|ft| name.starts_with(ft.as_str()) && name[ft.len()..].starts_with('_'))
}

/// Extracts the function name from a line like `    function bcs_serialize_Account(...)`.
fn extract_function_name(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if !trimmed.starts_with("function ") {
        return None;
    }
    let rest = trimmed.strip_prefix("function ")?;
    let name = rest.split('(').next()?.trim();
    if name.is_empty() {
        return None;
    }
    Some(name.to_string())
}

/// Checks if a function name is related to fungible-specific types.
/// Fungible functions are those whose name contains a fungible type name
/// (e.g. `bcs_serialize_FungibleOperation`, `FungibleOperation_case_transfer`).
fn is_fungible_function(fn_name: &str, fungible_types: &HashSet<String>) -> bool {
    fungible_types
        .iter()
        .any(|ft| fn_name.contains(ft.as_str()))
}

/// Extracts the type name from a struct definition line like "    struct Account {"
fn extract_struct_name(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.starts_with("struct ") && trimmed.ends_with('{') {
        let name = trimmed.strip_prefix("struct ")?.strip_suffix('{')?.trim();
        Some(name.to_string())
    } else {
        None
    }
}

/// Extracts the type name from an enum definition line.
/// Handles both multiline (`enum Foo {`) and single-line (`enum Foo { A, B }`) forms.
fn extract_enum_name(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if !trimmed.starts_with("enum ") {
        return None;
    }
    let rest = trimmed.strip_prefix("enum ")?;
    let name = rest.split(|c: char| c == '{' || c.is_whitespace()).next()?;
    if name.is_empty() {
        return None;
    }
    Some(name.to_string())
}

/// Skips a block of code (struct, function, enum) by counting braces.
/// Returns the index of the line after the closing brace.
fn skip_block(start: usize, lines: &[&str]) -> usize {
    let mut depth = 0;
    let mut i = start;
    while i < lines.len() {
        for ch in lines[i].chars() {
            if ch == '{' {
                depth += 1;
            } else if ch == '}' {
                depth -= 1;
                if depth == 0 {
                    return i + 1;
                }
            }
        }
        i += 1;
    }
    i
}

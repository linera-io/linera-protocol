// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test that compiled Wasm contracts don't use opcodes unsupported by testnet_conway validators.
//!
//! Rust 1.93 enabled `bulk-memory`, `bulk-memory-opt`, and `nontrapping-fptoint` by default
//! for `wasm32-unknown-unknown`. All three use opcode prefix 0xFC, which testnet_conway
//! validators do not support. Using a custom target JSON (wasm32-mvp.json) with those
//! features disabled and rebuilding std from source (build-std) is the fix.

#![cfg(not(target_arch = "wasm32"))]

use std::{path::PathBuf, process::Command};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..") // examples/fungible -> repo root
        .canonicalize()
        .expect("workspace root")
}

fn build_wasm(package: &str) -> PathBuf {
    let package_dir = workspace_root().join("examples").join(package);
    let manifest = package_dir.join("Cargo.toml");

    // The custom target disables bulk-memory, bulk-memory-opt, and nontrapping-fptoint.
    // -Z build-std rebuilds stdlib from source so it also omits those opcodes.
    // -Z json-target-spec is required to load a custom .json target file.
    let target_json = workspace_root()
        .join("linera-service")
        .join("wasm32-mvp.json");

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = Command::new(cargo)
        .current_dir(&package_dir)
        .args([
            "build",
            "--release",
            "--target",
            target_json.to_str().unwrap(),
            "-Z",
            "build-std=std,panic_abort",
            "-Z",
            "json-target-spec",
            "--manifest-path",
            manifest.to_str().unwrap(),
        ])
        .status()
        .expect("failed to spawn cargo build");

    assert!(status.success(), "cargo build failed for {package}");

    workspace_root()
        .join("examples")
        .join("target/wasm32-mvp/release")
        .join(format!("{}_contract.wasm", package.replace('-', "_")))
}

/// Wasm contracts must not use opcode 0xFC (bulk-memory / nontrapping-fptoint).
///
/// testnet_conway validators run an older Wasm runtime that rejects 0xFC as an unknown
/// opcode. We verify this by disassembling the binary and grepping for the specific
/// instruction names, which is more reliable than byte scanning.
#[test]
fn test_fungible_contract_has_no_opcode_0xfc() {
    let wasm_path = build_wasm("fungible");

    let output = Command::new("wasm2wat")
        .arg(&wasm_path)
        .output()
        .unwrap_or_else(|e| panic!("failed to run wasm2wat: {e}. Install wabt to run this test."));

    assert!(
        output.status.success(),
        "wasm2wat failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let wat = String::from_utf8_lossy(&output.stdout);
    let bad_instructions: Vec<&str> = wat
        .lines()
        .filter(|line| {
            line.contains("memory.copy")
                || line.contains("memory.fill")
                || line.contains("memory.init")
                || line.contains("trunc_sat")
        })
        .collect();

    assert!(
        bad_instructions.is_empty(),
        "fungible_contract.wasm contains {} instruction(s) using opcode 0xFC \
         (bulk-memory / nontrapping-fptoint). These are not supported by testnet_conway \
         validators. First few:\n{}",
        bad_instructions.len(),
        bad_instructions[..bad_instructions.len().min(5)].join("\n")
    );
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helpers for optimizing WebAssembly binaries.

use std::{
    io,
    path::{Path, PathBuf},
    process::Command,
};

/// Optimizes a single WebAssembly file in-place using `wasm-opt -O4`.
pub fn optimize_wasm_file(path: &Path) -> Result<(), io::Error> {
    let optimized_path = optimized_output_path(path)?;
    let output = Command::new("wasm-opt")
        .args(["-O4"])
        .arg(path)
        .arg("-o")
        .arg(&optimized_path)
        .output()?;

    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Failed to optimize Wasm binary with wasm-opt.\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }

    std::fs::rename(&optimized_path, path)?;
    Ok(())
}

/// Optimizes all `.wasm` files in the given directory in-place.
pub fn optimize_wasm_files_in(output_directory: &Path) -> Result<(), io::Error> {
    for entry in std::fs::read_dir(output_directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("wasm") {
            continue;
        }
        optimize_wasm_file(&path)?;
    }
    Ok(())
}

fn optimized_output_path(path: &Path) -> Result<PathBuf, io::Error> {
    if path.extension().and_then(|ext| ext.to_str()) != Some("wasm") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Expected a .wasm file, got {}", path.display()),
        ));
    }

    Ok(path.with_extension("wasm.opt"))
}

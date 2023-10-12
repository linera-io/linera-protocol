// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Context as _, Result};
use std::path::PathBuf;
use tokio::process::Command;
use tracing::{debug, error};

/// Attempts to resolve the path and test the version of the given binary against our
/// package version. This is meant for binaries of the Linera repository.
pub async fn resolve_binary(name: &'static str, package: &'static str) -> Result<PathBuf> {
    debug!("Resolving binary {name} based on the current binary path.");
    let mut current_binary_path = PathBuf::from(
        std::env::args()
            .next()
            .expect("args should start with the current binary path"),
    );
    current_binary_path.pop();

    #[cfg(any(test, feature = "test"))]
    // Test binaries are typically in target/debug/deps while crate binaries are in target/debug
    // (same thing for target/release).
    if current_binary_path.ends_with("target/debug/deps")
        || current_binary_path.ends_with("target/release/deps")
    {
        current_binary_path.pop();
    }

    let path = current_binary_path.join(name);
    let version = env!("CARGO_PKG_VERSION");
    if !path.exists() {
        error!(
            "Cannot find a binary {name} in the directory {}. \
             Consider using `cargo install {package}` or `cargo build -p {package}`",
            current_binary_path.display()
        );
        bail!("Failed to resolve binary {name}");
    }

    // Quick version check.
    let version_message = Command::new(&path)
        .arg("--version")
        .output()
        .await
        .with_context(|| {
            format!(
                "Failed to execute and retrieve version from the binary {name} in directory {}",
                current_binary_path.display()
            )
        })?
        .stdout;
    let found_version = String::from_utf8_lossy(&version_message)
        .trim()
        .split(' ')
        .last()
        .with_context(|| {
            format!(
                "Passing --version to the binary {name} in directory {} returned an empty result",
                current_binary_path.display()
            )
        })?
        .to_string();
    if version != found_version {
        error!("The binary {name} in directory {} should have version {version} (found {found_version}). \
                Consider using `cargo install {package} --version '{version}'` or `cargo build -p {package}`",
               current_binary_path.display()
        );
        bail!("Incorrect version for binary {name}");
    }

    Ok(path)
}

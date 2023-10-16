// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Context as _, Result};
use std::path::{Path, PathBuf};
use tokio::process::Command;
use tracing::{debug, error};

/// Attempts to resolve the path and test the version of the given binary against our
/// package version.
///
/// This is meant for binaries of the Linera repository. We use the current running binary
/// to locate the parent directory where to look for the given name.
pub async fn resolve_binary(name: &'static str, package: &'static str) -> Result<PathBuf> {
    let current_binary = std::env::current_exe()?;
    resolve_binary_in_same_directory_as(&current_binary, name, package).await
}

/// Same as [`resolve_binary`] but gives the option to specify a binary path to use as
/// reference. The path may be relative or absolute but it must point to a valid file on
/// disk.
pub async fn resolve_binary_in_same_directory_as<P: AsRef<Path>>(
    current_binary: P,
    name: &'static str,
    package: &'static str,
) -> Result<PathBuf> {
    let current_binary = current_binary.as_ref();
    debug!(
        "Resolving binary {name} based on the current binary path: {}",
        current_binary.display()
    );
    let mut current_binary_parent = current_binary
        .canonicalize()
        .with_context(|| format!("Failed to canonicalize '{}'", current_binary.display()))?;
    current_binary_parent.pop();

    #[cfg(any(test, feature = "test"))]
    // Test binaries are typically in target/debug/deps while crate binaries are in target/debug
    // (same thing for target/release).
    let current_binary_parent = if current_binary_parent.ends_with("target/debug/deps")
        || current_binary_parent.ends_with("target/release/deps")
    {
        PathBuf::from(current_binary_parent.parent().unwrap())
    } else {
        current_binary_parent
    };

    let binary = current_binary_parent.join(name);
    let version = env!("CARGO_PKG_VERSION");
    if !binary.exists() {
        error!(
            "Cannot find a binary {name} in the directory {}. \
             Consider using `cargo install {package}` or `cargo build -p {package}`",
            current_binary_parent.display()
        );
        bail!("Failed to resolve binary {name}");
    }

    // Quick version check.
    debug!("Checking the version of {}", binary.display());
    let version_message = Command::new(&binary)
        .arg("--version")
        .output()
        .await
        .with_context(|| {
            format!(
                "Failed to execute and retrieve version from the binary {name} in directory {}",
                current_binary_parent.display()
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
                current_binary_parent.display()
            )
        })?
        .to_string();
    if version != found_version {
        error!("The binary {name} in directory {} should have version {version} (found {found_version}). \
                Consider using `cargo install {package} --version '{version}'` or `cargo build -p {package}`",
               current_binary_parent.display()
        );
        bail!("Incorrect version for binary {name}");
    }
    debug!("{} has version {version}", binary.display());

    Ok(binary)
}

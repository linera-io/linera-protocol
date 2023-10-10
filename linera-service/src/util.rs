// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Result};
use once_cell::sync::OnceCell;
use std::{
    collections::{HashMap, HashSet},
    env,
    path::{Path, PathBuf},
};
use tokio::{process::Command, sync::Mutex};
use tracing::info;

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to `cargo` when starting client, server and proxy processes.
const CARGO_ENV: &str = "LINERA_CARGO_PARAMS";

pub fn resolve_cargo_binary(bin_name: &'static str) -> Result<PathBuf> {
    cargo_home().map(|cargo_home| cargo_home.join("bin").join(bin_name))
}

fn cargo_home() -> Result<PathBuf> {
    if let Ok(cargo_home) = std::env::var("CARGO_HOME") {
        Ok(PathBuf::from(cargo_home))
    } else if let Some(home) = dirs::home_dir() {
        Ok(home.join(".cargo"))
    } else {
        bail!("could not find CARGO_HOME directory, please specify it explicitly")
    }
}

#[allow(unused_mut)]
fn detect_current_features() -> Vec<&'static str> {
    let mut features = vec![];
    #[cfg(feature = "benchmark")]
    {
        features.push("benchmark");
    }
    #[cfg(feature = "wasmer")]
    {
        features.push("wasmer");
    }
    #[cfg(feature = "wasmtime")]
    {
        features.push("wasmtime");
    }
    #[cfg(feature = "rocksdb")]
    {
        features.push("rocksdb");
    }
    #[cfg(feature = "scylladb")]
    {
        features.push("scylladb");
    }
    #[cfg(feature = "aws")]
    {
        features.push("aws");
    }
    features
}

async fn cargo_force_build_binary(name: &'static str, package: Option<&'static str>) -> PathBuf {
    let package = package.unwrap_or(env!("CARGO_PKG_NAME"));
    let mut build_command = Command::new("cargo");
    build_command.args(["build", "-p", package]);
    let is_release = if let Ok(var) = env::var(CARGO_ENV) {
        let extra_args = var.split_whitespace();
        build_command.args(extra_args.clone());
        let extra_args: HashSet<_> = extra_args.into_iter().map(str::trim).collect();
        extra_args.contains("-r") || extra_args.contains("--release")
    } else {
        false
    };
    // Use the same features as the current environment so that we don't rebuild as often.
    let features = detect_current_features().join(",");
    build_command
        .arg("--no-default-features")
        .arg("--features")
        .arg(features);
    build_command.args(["--bin", name]);
    info!("Running compiler: {:?}", build_command);
    assert!(build_command
        .spawn()
        .unwrap()
        .wait()
        .await
        .unwrap()
        .success());
    let mut cargo_locate_command = Command::new("cargo");
    cargo_locate_command.args(["locate-project", "--workspace", "--message-format", "plain"]);
    let output = cargo_locate_command.output().await.unwrap().stdout;
    let workspace_path = Path::new(std::str::from_utf8(&output).unwrap().trim())
        .parent()
        .unwrap();
    if is_release {
        workspace_path
            .join("target/release")
            .join(name)
            .canonicalize()
            .unwrap()
    } else {
        workspace_path
            .join("target/debug")
            .join(name)
            .canonicalize()
            .unwrap()
    }
}

#[cfg(debug_assertions)]
pub async fn resolve_binary(name: &'static str, package: Option<&'static str>) -> Result<PathBuf> {
    Ok(cargo_build_binary(name, package).await)
}

#[cfg(not(debug_assertions))]
pub async fn resolve_binary(name: &'static str, _package: Option<&'static str>) -> Result<PathBuf> {
    resolve_cargo_binary(name)
}

async fn cargo_build_binary(name: &'static str, package: Option<&'static str>) -> PathBuf {
    type Key = (&'static str, Option<&'static str>);
    static COMPILED_BINARIES: OnceCell<Mutex<HashMap<Key, PathBuf>>> = OnceCell::new();
    let mut binaries = COMPILED_BINARIES.get_or_init(Default::default).lock().await;
    match binaries.get(&(name, package)) {
        Some(path) => path.clone(),
        None => {
            let path = cargo_force_build_binary(name, package).await;
            binaries.insert((name, package), path.clone());
            path
        }
    }
}

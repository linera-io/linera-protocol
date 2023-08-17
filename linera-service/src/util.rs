// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Result};
use std::path::PathBuf;

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

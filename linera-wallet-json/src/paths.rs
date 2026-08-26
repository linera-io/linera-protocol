// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Default file path resolution for wallet, keystore, and config directory.

use std::{env, path::PathBuf};

use anyhow::{anyhow, Error};
use tracing::{debug, info};

/// Resolves the default Linera config directory (`~/.config/linera`),
/// creating it if necessary.
pub fn config_dir() -> Result<PathBuf, Error> {
    let mut config_dir = dirs::config_dir().ok_or_else(|| {
        anyhow!(
            "Default wallet directory is not supported in this platform: \
             please specify storage and wallet paths"
        )
    })?;
    config_dir.push("linera");
    if !config_dir.exists() {
        debug!("Creating default wallet directory {}", config_dir.display());
        fs_err::create_dir_all(&config_dir)?;
    }
    info!("Using default wallet directory {}", config_dir.display());
    Ok(config_dir)
}

/// Resolves the wallet file path from an explicit path, environment variable,
/// or default location.
pub fn wallet_path(explicit: Option<&PathBuf>, suffix: &str) -> Result<PathBuf, Error> {
    if let Some(path) = explicit {
        return Ok(path.clone());
    }
    let wallet_env_var = env::var(format!("LINERA_WALLET{suffix}")).ok();
    if let Some(path) = wallet_env_var {
        return Ok(path.parse()?);
    }
    Ok(config_dir()?.join("wallet.json"))
}

/// Resolves the keystore file path from an explicit path, environment variable,
/// or default location.
pub fn keystore_path(explicit: Option<&PathBuf>, suffix: &str) -> Result<PathBuf, Error> {
    if let Some(path) = explicit {
        return Ok(path.clone());
    }
    let keystore_env_var = env::var(format!("LINERA_KEYSTORE{suffix}")).ok();
    if let Some(path) = keystore_env_var {
        return Ok(path.parse()?);
    }
    Ok(config_dir()?.join("keystore.json"))
}

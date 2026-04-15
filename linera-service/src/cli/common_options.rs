// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! CLI options shared between the Linera CLI and other tools (e.g. pm-benchmark).

use std::{env, path::PathBuf};

use anyhow::{bail, Error};
use linera_client::config::GenesisConfig;
use linera_execution::WasmRuntime;

use crate::{
    storage::{CommonStorageOptions, StorageConfig},
    Wallet,
};

/// Wallet, keystore, and storage configuration options common to all Linera client tools.
#[derive(Clone, clap::Parser)]
pub struct CommonCliOptions {
    /// Sets the file storing the private state of user chains (an empty one will be created
    /// if missing).
    #[arg(long = "wallet")]
    pub wallet_state_path: Option<PathBuf>,

    /// Sets the file storing the keystore state.
    #[arg(long = "keystore")]
    pub keystore_path: Option<PathBuf>,

    /// Given an ASCII alphanumeric parameter `X`, read the wallet state and the wallet
    /// storage config from the environment variables `LINERA_WALLET_{X}` and
    /// `LINERA_STORAGE_{X}` instead of `LINERA_WALLET` and
    /// `LINERA_STORAGE`.
    #[arg(long, short = 'w', value_parser = crate::util::parse_ascii_alphanumeric_string)]
    pub with_wallet: Option<String>,

    /// Storage configuration for the blockchain history.
    #[arg(long = "storage", global = true)]
    pub storage_config: Option<String>,

    /// Common storage options.
    #[command(flatten)]
    pub common_storage_options: CommonStorageOptions,

    /// The WebAssembly runtime to use.
    #[arg(long)]
    pub wasm_runtime: Option<WasmRuntime>,

    /// Output log messages from contract execution.
    #[arg(long = "with-application-logs", env = "LINERA_APPLICATION_LOGS")]
    pub application_logs: bool,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_CLIENT_TOKIO_THREADS")]
    pub tokio_threads: Option<usize>,

    /// The number of Tokio blocking threads to use.
    #[arg(long, env = "LINERA_CLIENT_TOKIO_BLOCKING_THREADS")]
    pub tokio_blocking_threads: Option<usize>,
}

impl CommonCliOptions {
    pub fn suffix(&self) -> String {
        self.with_wallet
            .as_ref()
            .map(|x| format!("_{}", x))
            .unwrap_or_default()
    }

    pub fn storage_config(&self) -> Result<StorageConfig, Error> {
        if let Some(config) = &self.storage_config {
            return config.parse();
        }
        let suffix = self.suffix();
        let storage_env_var = env::var(format!("LINERA_STORAGE{suffix}")).ok();
        if let Some(config) = storage_env_var {
            return config.parse();
        }
        cfg_if::cfg_if! {
            if #[cfg(feature = "rocksdb")] {
                let spawn_mode =
                    linera_views::rocks_db::RocksDbSpawnMode::get_spawn_mode_from_runtime();
                let inner_storage_config = crate::storage::InnerStorageConfig::RocksDb {
                    path: linera_wallet_json::paths::config_dir()?.join("wallet.db"),
                    spawn_mode,
                };
                let namespace = linera_storage::DEFAULT_NAMESPACE.to_string();
                Ok(StorageConfig {
                    inner_storage_config,
                    namespace,
                })
            } else {
                bail!("Cannot apply default storage because the feature 'rocksdb' was not selected");
            }
        }
    }

    pub fn wallet_path(&self) -> Result<PathBuf, Error> {
        linera_wallet_json::paths::wallet_path(self.wallet_state_path.as_ref(), &self.suffix())
    }

    pub fn keystore_path(&self) -> Result<PathBuf, Error> {
        linera_wallet_json::paths::keystore_path(self.keystore_path.as_ref(), &self.suffix())
    }

    pub fn wallet(&self) -> Result<Wallet, Error> {
        Ok(Wallet::read(&self.wallet_path()?)?)
    }

    pub fn keystore(&self) -> Result<linera_wallet_json::Keystore, Error> {
        Ok(linera_wallet_json::Keystore::read(&self.keystore_path()?)?)
    }

    pub fn create_wallet(&self, genesis_config: GenesisConfig) -> Result<Wallet, Error> {
        let wallet_path = self.wallet_path()?;
        if wallet_path.exists() {
            bail!("Wallet already exists: {}", wallet_path.display());
        }
        let wallet = Wallet::create(&wallet_path, genesis_config)?;
        wallet.save()?;
        Ok(wallet)
    }

    pub fn create_keystore(
        &self,
        testing_prng_seed: Option<u64>,
    ) -> Result<linera_wallet_json::Keystore, Error> {
        let keystore_path = self.keystore_path()?;
        if keystore_path.exists() {
            bail!("Keystore already exists: {}", keystore_path.display());
        }
        Ok(linera_wallet_json::Keystore::create(
            &keystore_path,
            testing_prng_seed,
        )?)
    }
}

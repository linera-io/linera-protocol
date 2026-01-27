// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{env, path::PathBuf};

use anyhow::{anyhow, bail, Error};
use linera_base::crypto::InMemorySigner;
use linera_client::{client_context::ClientContext, config::GenesisConfig};
use linera_execution::{WasmRuntime, WithWasmDefault as _};
use linera_persistent as persistent;
use linera_service::{
    cli::command::ClientCommand,
    storage::{CommonStorageOptions, Runnable, RunnableWithStore, StorageConfig},
    Wallet,
};
use tracing::{debug, info};

#[derive(Clone, clap::Parser)]
#[command(
    name = "linera",
    version = linera_version::VersionInfo::default_clap_str(),
    about = "Client implementation and command-line tool for the Linera blockchain",
)]
pub struct Options {
    /// Common options.
    #[command(flatten)]
    pub client_options: linera_client::Options,

    /// Sets the file storing the private state of user chains (an empty one will be created if missing)
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

    /// Enable OpenTelemetry Chrome JSON exporter for trace data analysis.
    #[arg(long)]
    pub chrome_trace_exporter: bool,

    /// Output file path for Chrome trace JSON format.
    /// Can be visualized in chrome://tracing or Perfetto UI.
    #[arg(long, env = "LINERA_CHROME_TRACE_FILE")]
    pub chrome_trace_file: Option<String>,

    /// OpenTelemetry OTLP exporter endpoint (requires opentelemetry feature).
    #[arg(long, env = "LINERA_OTLP_EXPORTER_ENDPOINT")]
    pub otlp_exporter_endpoint: Option<String>,

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

    /// Size of the block cache (default: 5000)
    #[arg(long, env = "LINERA_BLOCK_CACHE_SIZE", default_value = "5000")]
    pub block_cache_size: usize,

    /// Size of the execution state cache (default: 10000)
    #[arg(
        long,
        env = "LINERA_EXECUTION_STATE_CACHE_SIZE",
        default_value = "10000"
    )]
    pub execution_state_cache_size: usize,

    /// Enable memory profiling (requires jemalloc feature and metrics).
    /// Exposes /debug/pprof and /debug/flamegraph endpoints on the metrics server.
    #[arg(long, env = "LINERA_ENABLE_MEMORY_PROFILING")]
    pub enable_memory_profiling: bool,

    /// Subcommand.
    #[command(subcommand)]
    pub command: ClientCommand,
}

impl Options {
    pub fn init() -> Self {
        <Options as clap::Parser>::parse()
    }

    pub async fn create_client_context<S, Si>(
        &self,
        storage: S,
        wallet: Wallet,
        signer: Si,
    ) -> anyhow::Result<
        ClientContext<linera_core::environment::Impl<S, linera_rpc::NodeProvider, Si, Wallet>>,
    >
    where
        S: linera_core::environment::Storage,
        Si: linera_core::environment::Signer,
    {
        let genesis_config = wallet.genesis_config().clone();
        let default_chain = wallet.default_chain();
        Ok(ClientContext::new(
            storage,
            wallet,
            signer,
            &self.client_options,
            default_chain,
            genesis_config,
            self.block_cache_size,
            self.execution_state_cache_size,
        )
        .await?)
    }

    pub async fn run_with_storage<R: Runnable>(&self, job: R) -> Result<R::Output, Error> {
        let storage_config = self.storage_config()?;
        debug!("Running command using storage configuration: {storage_config}");
        let store_config =
            storage_config.add_common_storage_options(&self.common_storage_options)?;
        let output = Box::pin(store_config.run_with_storage(
            self.wasm_runtime.with_wasm_default(),
            self.application_logs,
            job,
        ))
        .await?;
        Ok(output)
    }

    pub async fn run_with_store<R: RunnableWithStore>(&self, job: R) -> Result<R::Output, Error> {
        let storage_config = self.storage_config()?;
        debug!("Running command using storage configuration: {storage_config}");
        let store_config =
            storage_config.add_common_storage_options(&self.common_storage_options)?;
        let output = Box::pin(store_config.run_with_store(job)).await?;
        Ok(output)
    }

    pub async fn initialize_storage(&self) -> Result<(), Error> {
        let storage_config = self.storage_config()?;
        debug!("Initializing storage using configuration: {storage_config}");
        let store_config =
            storage_config.add_common_storage_options(&self.common_storage_options)?;
        let wallet = self.wallet()?;
        store_config.initialize(wallet.genesis_config()).await?;
        Ok(())
    }

    pub fn wallet(&self) -> Result<Wallet, Error> {
        Ok(Wallet::read(&self.wallet_path()?)?)
    }

    pub fn signer(&self) -> Result<persistent::File<InMemorySigner>, Error> {
        Ok(persistent::File::read(&self.keystore_path()?)?)
    }

    pub fn suffix(&self) -> String {
        self.with_wallet
            .as_ref()
            .map(|x| format!("_{}", x))
            .unwrap_or_default()
    }

    pub fn config_path() -> Result<PathBuf, Error> {
        let mut config_dir = dirs::config_dir().ok_or_else(|| anyhow!(
            "Default wallet directory is not supported in this platform: please specify storage and wallet paths"
        ))?;
        config_dir.push("linera");
        if !config_dir.exists() {
            debug!("Creating default wallet directory {}", config_dir.display());
            fs_err::create_dir_all(&config_dir)?;
        }
        info!("Using default wallet directory {}", config_dir.display());
        Ok(config_dir)
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
                let inner_storage_config = linera_service::storage::InnerStorageConfig::RocksDb {
                    path: Self::config_path()?.join("wallet.db"),
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
        if let Some(path) = &self.wallet_state_path {
            return Ok(path.clone());
        }
        let suffix = self.suffix();
        let wallet_env_var = env::var(format!("LINERA_WALLET{suffix}")).ok();
        if let Some(path) = wallet_env_var {
            return Ok(path.parse()?);
        }
        let config_path = Self::config_path()?;
        Ok(config_path.join("wallet.json"))
    }

    pub fn keystore_path(&self) -> Result<PathBuf, Error> {
        if let Some(path) = &self.keystore_path {
            return Ok(path.clone());
        }
        let suffix = self.suffix();
        let keystore_env_var = env::var(format!("LINERA_KEYSTORE{suffix}")).ok();
        if let Some(path) = keystore_env_var {
            return Ok(path.parse()?);
        }
        let config_path = Self::config_path()?;
        Ok(config_path.join("keystore.json"))
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
    ) -> Result<persistent::File<InMemorySigner>, Error> {
        let keystore_path = self.keystore_path()?;
        if keystore_path.exists() {
            bail!("Keystore already exists: {}", keystore_path.display());
        }
        Ok(persistent::File::read_or_create(&keystore_path, || {
            Ok(InMemorySigner::new(testing_prng_seed))
        })?)
    }
}

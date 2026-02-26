// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::Error;
use linera_base::crypto::InMemorySigner;
use linera_client::{client_context::ClientContext, config::GenesisConfig};
use linera_execution::WithWasmDefault as _;
use linera_persistent as persistent;
use linera_service::{
    cli::{command::ClientCommand, common_options::CommonCliOptions},
    storage::{Runnable, RunnableWithStore, StorageConfig},
    Wallet,
};
use tracing::debug;

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

    /// Common wallet, keystore, and storage options.
    #[command(flatten)]
    pub common: CommonCliOptions,

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
        )
        .await?)
    }

    pub async fn run_with_storage<R: Runnable>(&self, job: R) -> Result<R::Output, Error> {
        let storage_config = self.storage_config()?;
        debug!("Running command using storage configuration: {storage_config}");
        let store_config =
            storage_config.add_common_storage_options(&self.common.common_storage_options)?;
        let output = Box::pin(store_config.run_with_storage(
            self.common.wasm_runtime.with_wasm_default(),
            self.common.application_logs,
            job,
        ))
        .await?;
        Ok(output)
    }

    pub async fn run_with_store<R: RunnableWithStore>(
        &self,
        assert_storage_v1: bool,
        need_migration: bool,
        job: R,
    ) -> Result<R::Output, Error> {
        let storage_config = self.storage_config()?;
        debug!("Running command using storage configuration: {storage_config}");
        let store_config =
            storage_config.add_common_storage_options(&self.common.common_storage_options)?;
        if assert_storage_v1 {
            store_config
                .clone()
                .run_with_store(crate::AssertStorageV1)
                .await?;
        }
        if need_migration {
            store_config
                .clone()
                .run_with_store(crate::StorageMigration)
                .await?;
        }
        let output = Box::pin(store_config.run_with_store(job)).await?;
        Ok(output)
    }

    pub async fn initialize_storage(&self) -> Result<(), Error> {
        let storage_config = self.storage_config()?;
        debug!("Initializing storage using configuration: {storage_config}");
        let store_config =
            storage_config.add_common_storage_options(&self.common.common_storage_options)?;
        let wallet = self.wallet()?;
        store_config.initialize(wallet.genesis_config()).await?;
        Ok(())
    }

    // Delegation methods to CommonCliOptions, keeping the existing API surface
    // for call sites in main.rs.

    fn storage_config(&self) -> Result<StorageConfig, Error> {
        self.common.storage_config()
    }

    pub fn wallet_path(&self) -> Result<PathBuf, Error> {
        self.common.wallet_path()
    }

    pub fn wallet(&self) -> Result<Wallet, Error> {
        self.common.wallet()
    }

    pub fn signer(&self) -> Result<persistent::File<InMemorySigner>, Error> {
        self.common.signer()
    }

    pub fn create_wallet(&self, genesis_config: GenesisConfig) -> Result<Wallet, Error> {
        self.common.create_wallet(genesis_config)
    }

    pub fn create_keystore(
        &self,
        testing_prng_seed: Option<u64>,
    ) -> Result<persistent::File<InMemorySigner>, Error> {
        self.common.create_keystore(testing_prng_seed)
    }
}

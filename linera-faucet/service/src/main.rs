// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

use std::{path::PathBuf, sync::Arc};

use anyhow::{bail, Context, Error};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clap::Parser;
use linera_base::{
    crypto::InMemorySigner,
    data_types::{Amount, Timestamp},
    identifiers::ChainId,
    listen_for_shutdown_signals,
};
use linera_client::{
    chain_listener::ChainListenerConfig, client_context::ClientContext,
    client_options::ClientContextOptions, wallet::Wallet,
};
use linera_execution::{WasmRuntime, WithWasmDefault as _};
use linera_faucet_server::{FaucetConfig, FaucetService};
use linera_persistent::{self as persistent, Persist};
use linera_service::storage::{CommonStorageOptions, Runnable, StorageConfig};
use linera_storage::Storage;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Parser)]
#[command(
    name = "linera-faucet",
    about = "Run a GraphQL service that exposes a faucet where users can claim tokens"
)]
struct FaucetOptions {
    /// The chain that gives away its tokens.
    #[arg(long)]
    chain_id: Option<ChainId>,

    /// The port on which to run the server
    #[arg(long, default_value = "8080")]
    port: u16,

    /// The port for prometheus to scrape.
    #[cfg(with_metrics)]
    #[arg(long, default_value = "9090")]
    metrics_port: u16,

    /// The number of tokens to send to each new chain.
    #[arg(long)]
    amount: Amount,

    /// The end timestamp: The faucet will rate-limit the token supply so it runs out of money
    /// no earlier than this.
    #[arg(long)]
    limit_rate_until: Option<DateTime<Utc>>,

    /// Configuration for the faucet chain listener.
    #[command(flatten)]
    config: ChainListenerConfig,

    /// Path to the persistent storage file for faucet mappings.
    #[arg(long)]
    storage_path: PathBuf,

    /// Maximum number of operations to include in a single block (default: 100).
    #[arg(long, default_value = "100")]
    max_batch_size: usize,

    /// Storage configuration for the blockchain history.
    #[arg(long = "storage")]
    storage_config: Option<String>,

    /// Common storage options.
    #[command(flatten)]
    common_storage_options: CommonStorageOptions,

    /// Common client context options.
    #[command(flatten)]
    context_options: ClientContextOptions,

    /// The WebAssembly runtime to use.
    #[arg(long)]
    wasm_runtime: Option<WasmRuntime>,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_FAUCET_TOKIO_THREADS")]
    tokio_threads: Option<usize>,

    /// The number of Tokio blocking threads to use.
    #[arg(long, env = "LINERA_FAUCET_TOKIO_BLOCKING_THREADS")]
    tokio_blocking_threads: Option<usize>,
}

impl FaucetOptions {
    fn storage_config(&self) -> Result<StorageConfig, Error> {
        if let Some(config) = &self.storage_config {
            return config.parse();
        }
        if let Ok(value) = std::env::var("LINERA_STORAGE") {
            return value.parse();
        }
        bail!("Either `--storage` or the LINERA_STORAGE environment variable must be set")
    }

    fn wallet(&self) -> Result<persistent::File<Wallet>, Error> {
        if let Ok(value) = std::env::var("LINERA_WALLET") {
            Ok(persistent::File::read(&PathBuf::from(value)).context("Unable to read wallet")?)
        } else {
            bail!("The LINERA_WALLET environment variable must be set")
        }
    }

    fn signer(&self) -> Result<persistent::File<InMemorySigner>, Error> {
        if let Ok(value) = std::env::var("LINERA_KEYSTORE") {
            Ok(persistent::File::read(&PathBuf::from(value)).context("Unable to read keystore")?)
        } else {
            bail!("The LINERA_KEYSTORE environment variable must be set")
        }
    }
}

struct FaucetJob(FaucetOptions);

#[async_trait]
impl Runnable for FaucetJob {
    type Output = anyhow::Result<()>;

    async fn run<S>(self, storage: S) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let FaucetJob(options) = self;
        let wallet = options.wallet()?;
        let signer = options.signer()?;

        let context = ClientContext::new(
            storage.clone(),
            options.context_options.clone(),
            wallet,
            signer.into_value(),
        );

        let chain_id = options
            .chain_id
            .unwrap_or_else(|| context.first_non_admin_chain());
        info!("Starting faucet service using chain {}", chain_id);

        let end_timestamp = options.limit_rate_until.map_or_else(Timestamp::now, |et| {
            let micros = u64::try_from(et.timestamp_micros()).expect("End timestamp before 1970");
            Timestamp::from(micros)
        });

        let genesis_config = Arc::new(context.wallet().genesis_config().clone());
        let config = FaucetConfig {
            port: options.port,
            #[cfg(with_metrics)]
            metrics_port: options.metrics_port,
            chain_id,
            amount: options.amount,
            end_timestamp,
            genesis_config,
            chain_listener_config: options.config,
            storage_path: options.storage_path,
            max_batch_size: options.max_batch_size,
        };

        let faucet = FaucetService::new(config, context, storage).await?;
        let cancellation_token = CancellationToken::new();
        let child_token = cancellation_token.child_token();
        tokio::spawn(listen_for_shutdown_signals(cancellation_token));
        faucet.run(child_token).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    use is_terminal::IsTerminal as _;

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .init();

    let options = FaucetOptions::parse();

    let storage_config = options.storage_config()?;
    let store_config =
        storage_config.add_common_storage_options(&options.common_storage_options)?;

    store_config
        .run_with_storage(options.wasm_runtime.with_wasm_default(), FaucetJob(options))
        .await?
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One-shot CLI that provisions a Linera wallet + keystore + chain for
//! `linera-bridge serve`.
//!
//! Flow:
//!   1. Fetch genesis config from the faucet.
//!   2. Create `wallet.json` using that genesis config.
//!   3. Create `keystore.json` and generate a signing key.
//!   4. Claim a chain from the faucet using that key.
//!   5. Print `{ "chain_id": ..., "owner": ... }` on stdout so shell wrappers
//!      can feed the values into `linera-bridge serve`.
//!
//! Storage (rocksdb) is NOT initialized here; `linera-bridge serve` initializes
//! it on first startup.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use linera_base::identifiers::AccountOwner;
use linera_faucet_client::Faucet;
use linera_wallet_json::{Keystore, PersistentWallet};

#[derive(Parser, Debug)]
#[command(name = "linera-bridge-init")]
struct Cli {
    /// URL of the Linera faucet (e.g. http://localhost:8080).
    #[arg(long)]
    faucet_url: String,

    /// Path to the wallet state file to create.
    #[arg(long = "wallet", env = "LINERA_WALLET")]
    wallet: PathBuf,

    /// Path to the keystore file to create or extend.
    #[arg(long = "keystore", env = "LINERA_KEYSTORE")]
    keystore: PathBuf,
}

fn main() -> Result<()> {
    linera_base::tracing::init("linera-bridge-init");
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(cli.run())
}

impl Cli {
    async fn run(self) -> Result<()> {
        anyhow::ensure!(
            !self.wallet.exists(),
            "wallet already exists at {}; refusing to overwrite",
            self.wallet.display(),
        );

        tracing::info!(url = %self.faucet_url, "Fetching genesis config from faucet");
        let faucet = Faucet::new(self.faucet_url);
        let genesis_config = faucet
            .genesis_config()
            .await
            .context("failed to fetch genesis config from faucet")?;

        tracing::info!(wallet = %self.wallet.display(), "Creating wallet");
        PersistentWallet::create(&self.wallet, genesis_config)
            .context("failed to create wallet")?;

        tracing::info!(keystore = %self.keystore.display(), "Preparing keystore");
        let mut keystore =
            Keystore::create(&self.keystore, None).context("failed to open or create keystore")?;
        let owner = AccountOwner::from(
            keystore
                .generate_key()
                .await
                .context("failed to generate signing key")?,
        );

        tracing::info!(%owner, "Claiming chain from faucet");
        let chain_desc = faucet
            .claim(&owner)
            .await
            .context("failed to claim chain from faucet")?;
        let chain_id = chain_desc.id();
        tracing::info!(%chain_id, %owner, "Chain claimed");

        let payload = serde_json::json!({
            "chain_id": chain_id.to_string(),
            "owner": owner.to_string(),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);

        Ok(())
    }
}

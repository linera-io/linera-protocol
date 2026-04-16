// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! CLI tool for Linera EVM bridge operations.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

/// Linera Bridge CLI
#[derive(Parser, Debug)]
#[command(name = "linera-bridge")]
enum Cli {
    /// Query a Linera faucet and output LightClient constructor args for EVM deployment
    InitLightClient(InitLightClientOptions),
    /// Generate a deposit proof for a given EVM transaction
    GenerateDepositProof(GenerateDepositProofOptions),
    /// Run the relay server (proof generation + chain inbox processing + EVM forwarding)
    #[cfg(feature = "relay")]
    Serve(Box<ServeOptions>),
}

#[derive(clap::Args, Debug, Clone)]
struct InitLightClientOptions {
    /// URL of the Linera faucet (e.g. http://localhost:8080)
    #[arg(long)]
    faucet_url: String,

    /// Path to write the constructor args JSON file
    #[arg(long, default_value = "light-client-args.json")]
    output: PathBuf,
}

#[derive(clap::Args, Debug, Clone)]
struct GenerateDepositProofOptions {
    /// EVM JSON-RPC URL (e.g. https://mainnet.base.org)
    #[arg(long)]
    rpc_url: String,

    /// Transaction hash containing the DepositInitiated event
    #[arg(long)]
    tx_hash: String,

    /// Path to write the proof JSON file
    #[arg(long, default_value = "deposit-proof.json")]
    output: PathBuf,
}

#[cfg(feature = "relay")]
#[derive(clap::Args, Debug, Clone)]
struct ServeOptions {
    /// EVM JSON-RPC URL (e.g. http://localhost:8545)
    #[arg(long)]
    rpc_url: String,

    /// URL of the Linera faucet (required when wallet doesn't exist or chain ID not provided)
    #[arg(long)]
    faucet_url: Option<String>,

    /// Path to the wallet state file.
    #[arg(long = "wallet", env = "LINERA_WALLET")]
    wallet: Option<PathBuf>,

    /// Path to the keystore file.
    #[arg(long = "keystore", env = "LINERA_KEYSTORE")]
    keystore: Option<PathBuf>,

    /// Storage configuration for blockchain history (e.g. rocksdb:/path/to/db).
    #[arg(long = "storage", env = "LINERA_STORAGE")]
    storage: Option<String>,

    /// Linera bridge chain ID. If omitted, claims a new chain from faucet.
    #[arg(long)]
    linera_bridge_chain_id: Option<linera_base::identifiers::ChainId>,

    /// Owner to use for the bridge chain. Required when --linera-bridge-chain-id is provided.
    #[arg(long, requires = "linera_bridge_chain_id")]
    linera_bridge_chain_owner: Option<linera_base::identifiers::AccountOwner>,

    /// Address of the FungibleBridge contract on EVM.
    #[arg(long)]
    evm_bridge_address: String,

    /// evm-bridge Linera ApplicationId (hex).
    #[arg(long)]
    linera_bridge_address: String,

    /// wrapped-fungible Linera ApplicationId (hex).
    #[arg(long)]
    linera_fungible_address: String,

    /// Address of the LightClient contract on EVM.
    /// When provided, skips discovering it via FungibleBridge.lightClient().
    #[arg(long)]
    evm_light_client_address: Option<String>,

    /// EVM private key for signing addBlock transactions
    #[arg(long)]
    evm_private_key: String,

    /// Port to listen on for HTTP requests
    #[arg(long, default_value = "3001")]
    port: u16,

    /// The maximal number of entries in the blob cache.
    #[arg(long, default_value = "1000")]
    blob_cache_size: usize,

    /// The maximal number of entries in the confirmed block cache.
    #[arg(long, default_value = "1000")]
    confirmed_block_cache_size: usize,

    /// The maximal number of entries in the certificate cache.
    #[arg(long, default_value = "1000")]
    certificate_cache_size: usize,

    /// The maximal number of entries in the raw certificate cache.
    #[arg(long, default_value = "1000")]
    certificate_raw_cache_size: usize,

    /// The maximal number of entries in the event cache.
    #[arg(long, default_value = "1000")]
    event_cache_size: usize,

    /// Interval between monitor scan loops, in seconds.
    #[arg(long, default_value = "30")]
    monitor_scan_interval: u64,

    /// EVM block number to start scanning from for deposit monitoring.
    #[arg(long, default_value = "0")]
    monitor_start_block: u64,

    /// Maximum number of retry attempts for pending deposits and burns.
    #[arg(long, default_value = "10")]
    max_retries: u32,

    /// Path to the SQLite database for persistent request storage.
    /// Defaults to `bridge_relay.sqlite3` next to the RocksDB storage directory.
    #[arg(long)]
    sqlite_path: Option<std::path::PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    match cli {
        Cli::InitLightClient(options) => runtime.block_on(options.run()),
        Cli::GenerateDepositProof(options) => runtime.block_on(options.run()),
        #[cfg(feature = "relay")]
        Cli::Serve(options) => runtime.block_on(options.run()),
    }
}

#[cfg(feature = "relay")]
impl ServeOptions {
    async fn run(&self) -> Result<()> {
        linera_base::tracing::init("linera-bridge");

        // Tonic pulls in rustls 0.23 which requires an explicit crypto provider.
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("failed to install rustls crypto provider");

        Box::pin(linera_bridge::relay::run(
            &self.rpc_url,
            self.faucet_url.as_deref(),
            self.wallet.as_deref(),
            self.keystore.as_deref(),
            self.storage.as_deref(),
            self.linera_bridge_chain_id,
            self.linera_bridge_chain_owner,
            &self.evm_bridge_address,
            &self.linera_bridge_address,
            &self.linera_fungible_address,
            &self.evm_private_key,
            self.evm_light_client_address.as_deref(),
            self.port,
            linera_storage::StorageCacheConfig {
                blob_cache_size: self.blob_cache_size,
                confirmed_block_cache_size: self.confirmed_block_cache_size,
                certificate_cache_size: self.certificate_cache_size,
                certificate_raw_cache_size: self.certificate_raw_cache_size,
                event_cache_size: self.event_cache_size,
                cache_cleanup_interval_secs: linera_storage::DEFAULT_CLEANUP_INTERVAL_SECS,
            },
            self.monitor_scan_interval,
            self.monitor_start_block,
            self.max_retries,
            self.sqlite_path.as_deref(),
        ))
        .await
    }
}

impl GenerateDepositProofOptions {
    async fn run(&self) -> Result<()> {
        use alloy_primitives::B256;
        use linera_bridge::proof::gen::{DepositProofClient, HttpDepositProofClient};

        let tx_hash: B256 = self
            .tx_hash
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid tx hash: {}", self.tx_hash))?;

        eprintln!("Generating deposit proof for tx {}...", self.tx_hash);

        let client = HttpDepositProofClient::new(&self.rpc_url)?;
        let proof = client.generate_deposit_proof(tx_hash).await?;

        let result = serde_json::json!({
            "block_header_rlp": alloy_primitives::hex::encode_prefixed(&proof.block_header_rlp),
            "receipt_rlp": alloy_primitives::hex::encode_prefixed(&proof.receipt_rlp),
            "proof_nodes": proof.proof_nodes.iter()
                .map(alloy_primitives::hex::encode_prefixed)
                .collect::<Vec<_>>(),
            "tx_index": proof.tx_index,
            "log_indices": proof.log_indices,
        });

        let json_str = serde_json::to_string_pretty(&result)?;
        eprintln!("Writing deposit proof to {:?}", self.output);
        fs_err::write(&self.output, &json_str)?;
        println!("{json_str}");

        Ok(())
    }
}

impl InitLightClientOptions {
    async fn run(&self) -> Result<()> {
        use std::collections::BTreeMap;

        use linera_base::{
            crypto::ValidatorPublicKey,
            data_types::{ChainDescription, ChainOrigin, Epoch},
        };
        use linera_bridge::evm::client::validator_evm_address;
        use linera_execution::committee::ValidatorState;

        // Response types for the combined GraphQL query.
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct GqlResponse {
            data: GqlData,
        }

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct GqlData {
            current_committee: CommitteeResponse,
            current_epoch: Epoch,
            genesis_config: GenesisResponse,
        }

        #[derive(serde::Deserialize)]
        struct CommitteeResponse {
            validators: BTreeMap<ValidatorPublicKey, ValidatorState>,
        }

        // Only the fields we need from genesisConfig.
        #[derive(serde::Deserialize)]
        struct GenesisResponse {
            chains: Vec<ChainDescription>,
        }

        eprintln!(
            "Querying faucet at {} for current committee...",
            self.faucet_url
        );
        let client = reqwest::Client::new();

        let resp = client
            .post(&self.faucet_url)
            .json(&serde_json::json!({
                "query": "{ currentCommittee { validators } currentEpoch genesisConfig }"
            }))
            .send()
            .await?
            .json::<GqlResponse>()
            .await?;

        let admin_chain_id = resp
            .data
            .genesis_config
            .chains
            .iter()
            .find(|c| c.origin() == ChainOrigin::Root(0))
            .ok_or_else(|| anyhow::anyhow!("no admin chain (Root(0)) in genesis config"))?
            .id();
        let admin_chain_bytes = <[u8; 32]>::from(*admin_chain_id.0.as_bytes());

        let mut validators: Vec<String> = Vec::new();
        let mut weights: Vec<u64> = Vec::new();

        for (public_key, state) in &resp.data.current_committee.validators {
            let address = validator_evm_address(public_key);
            validators.push(format!("{address}"));
            weights.push(state.votes);
        }

        let result = serde_json::json!({
            "validators": validators,
            "weights": weights,
            "admin_chain_id": format!("0x{}", alloy_primitives::hex::encode(admin_chain_bytes)),
            "epoch": resp.data.current_epoch,
        });

        let json_str = serde_json::to_string_pretty(&result)?;
        eprintln!("Writing constructor args to {:?}", self.output);
        fs_err::write(&self.output, &json_str)?;
        println!("{json_str}");

        Ok(())
    }
}

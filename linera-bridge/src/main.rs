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

fn main() -> Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    match cli {
        Cli::InitLightClient(options) => runtime.block_on(options.run()),
        Cli::GenerateDepositProof(options) => runtime.block_on(options.run()),
    }
}

impl GenerateDepositProofOptions {
    async fn run(&self) -> Result<()> {
        use alloy_primitives::B256;
        use linera_bridge::proof_gen::{DepositProofClient, HttpDepositProofClient};

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
                .map(|n| alloy_primitives::hex::encode_prefixed(n))
                .collect::<Vec<_>>(),
            "tx_index": proof.tx_index,
            "log_index": proof.log_index,
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
        use linera_bridge::evm_client::validator_evm_address;
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

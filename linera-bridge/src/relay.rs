// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Relay server for the EVM↔Linera bridge demo.
//!
//! Three responsibilities:
//! - **HTTP**: `POST /generate-proof` — generates MPT deposit proofs for browser clients.
//! - **Linera client**: claims a "bridge chain", listens for `NewIncomingBundle` notifications,
//!   and processes the inbox so that cross-chain Credit messages are executed.
//! - **EVM forwarder**: after processing inbox, BCS-serializes the resulting certificate and
//!   calls `FungibleBridge.addBlock(bytes)` on the EVM chain.

use std::{sync::Arc, time::Duration};

use alloy::{
    network::EthereumWallet, primitives::Address, providers::ProviderBuilder,
    signers::local::PrivateKeySigner, sol,
};
use anyhow::{Context as _, Result};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use futures::StreamExt as _;
use linera_base::{crypto::InMemorySigner, identifiers::AccountOwner};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{environment::wallet::Memory, worker::Reason};
use linera_execution::WasmRuntime;
use linera_faucet_client::Faucet;
use linera_storage::DbStorage;
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use tower_http::cors::CorsLayer;
use tracing;

use crate::proof::gen::{DepositProofClient, HttpDepositProofClient};

// ── Alloy ABI for FungibleBridge.addBlock ──

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
    }
}

// ── Shared state for the HTTP server ──

struct AppState {
    proof_client: HttpDepositProofClient,
}

// ── HTTP handlers ──

#[derive(serde::Deserialize)]
struct GenerateProofRequest {
    tx_hash: String,
}

async fn generate_proof(
    State(state): State<Arc<AppState>>,
    Json(req): Json<GenerateProofRequest>,
) -> impl IntoResponse {
    let tx_hash = match req.tx_hash.parse() {
        Ok(h) => h,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid tx_hash"})),
            )
        }
    };

    match state.proof_client.generate_deposit_proof(tx_hash).await {
        Ok(proof) => {
            let result = serde_json::json!({
                "block_header_rlp": alloy_primitives::hex::encode_prefixed(&proof.block_header_rlp),
                "receipt_rlp": alloy_primitives::hex::encode_prefixed(&proof.receipt_rlp),
                "proof_nodes": proof.proof_nodes.iter()
                    .map(|n| alloy_primitives::hex::encode_prefixed(n))
                    .collect::<Vec<_>>(),
                "tx_index": proof.tx_index,
                "log_index": proof.log_index,
            });
            (StatusCode::OK, Json(result))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("{e:#}")})),
        ),
    }
}

// ── Entry point ──

/// Resolve bridge address: use CLI arg if provided, otherwise poll a file.
async fn resolve_bridge_address(
    bridge_address: Option<&str>,
    bridge_address_file: &str,
) -> Result<Address> {
    if let Some(addr) = bridge_address {
        return addr.parse().context("invalid bridge address");
    }

    tracing::info!(
        file = bridge_address_file,
        "Bridge address not provided, polling file..."
    );
    loop {
        if let Ok(contents) = tokio::fs::read_to_string(bridge_address_file).await {
            let addr_str = contents.trim();
            if !addr_str.is_empty() {
                let addr: Address = addr_str.parse().context("invalid bridge address in file")?;
                tracing::info!(%addr, "Read bridge address from file");
                return Ok(addr);
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

pub async fn run(
    rpc_url: &str,
    faucet_url: &str,
    bridge_address: Option<&str>,
    bridge_address_file: &str,
    evm_private_key: &str,
    port: u16,
) -> Result<()> {
    tracing_subscriber::fmt::init();

    tracing::info!("Starting bridge relay server...");

    // ── 1. Set up Linera client ──
    tracing::info!("Connecting to Linera faucet at {faucet_url}...");
    let faucet = Faucet::new(faucet_url.to_string());
    let genesis_config = faucet.genesis_config().await?;

    let config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &config,
        "bridge-relay",
        Some(WasmRuntime::default()),
    )
    .await?;

    genesis_config.initialize_storage(&mut storage).await?;

    let mut signer = InMemorySigner::new(None);

    let mut ctx = ClientContext::new(
        storage,
        Memory::default(),
        signer.clone(),
        &Default::default(),
        None,
        genesis_config,
    )
    .await?;

    // ── 2. Claim bridge chain ──
    tracing::info!("Claiming bridge chain from faucet...");
    let owner = AccountOwner::from(signer.generate_new());
    let chain_desc = faucet.claim(&owner).await?;
    let chain_id = chain_desc.id();
    ctx.extend_with_chain(chain_desc, Some(owner)).await?;

    let chain_client = ctx.make_chain_client(chain_id).await?;
    chain_client.synchronize_from_validators().await?;

    tracing::info!(%chain_id, "Bridge chain claimed");

    // Write chain ID to /shared/bridge-chain-id if the directory exists (Docker mode).
    if let Ok(()) = fs_err::write("/shared/bridge-chain-id", chain_id.to_string()) {
        tracing::info!("Wrote bridge chain ID to /shared/bridge-chain-id");
    }

    // ── 3. Set up EVM provider ──
    // Resolve bridge address (may poll a file if not provided via CLI).
    let bridge_addr = resolve_bridge_address(bridge_address, bridge_address_file).await?;
    let evm_signer: PrivateKeySigner =
        evm_private_key.parse().context("invalid EVM private key")?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .connect_http(rpc_url.parse().context("invalid RPC URL")?);

    // ── 4. Start notification listener ──
    let mut notifications = chain_client.subscribe()?;
    let (listener, _abort_handle, _) = chain_client.listen().await?;
    tokio::spawn(listener);

    // ── 5. Start HTTP server ──
    let proof_client = HttpDepositProofClient::new(rpc_url)?;
    let app_state = Arc::new(AppState { proof_client });

    let app = Router::new()
        .route("/generate-proof", post(generate_proof))
        .layer(CorsLayer::permissive())
        .with_state(app_state);

    let bind_addr = format!("0.0.0.0:{port}");
    tracing::info!("HTTP server listening on {bind_addr}");

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("HTTP server error: {e}");
        }
    });

    // ── 6. Background loop: process inbox → forward blocks ──
    tracing::info!("Listening for NewIncomingBundle notifications...");
    loop {
        let notification =
            match tokio::time::timeout(Duration::from_secs(300), notifications.next()).await {
                Ok(Some(n)) => n,
                Ok(None) => {
                    tracing::warn!("Notification stream ended, exiting");
                    break;
                }
                Err(_) => {
                    // Timeout — just keep polling.
                    continue;
                }
            };

        if !matches!(notification.reason, Reason::NewIncomingBundle { .. }) {
            continue;
        }

        tracing::info!("Received NewIncomingBundle, processing inbox...");

        if let Err(e) = chain_client.synchronize_from_validators().await {
            tracing::error!("Failed to synchronize: {e}");
            continue;
        }

        let certs = match chain_client.process_inbox().await {
            Ok((certs, _)) => certs,
            Err(e) => {
                tracing::error!("Failed to process inbox: {e}");
                continue;
            }
        };

        if certs.is_empty() {
            tracing::info!("No certificates from inbox processing");
            continue;
        }

        tracing::info!(count = certs.len(), "Processed inbox certificates");

        // Forward each certificate to EVM.
        for cert in &certs {
            let cert_bytes = match bcs::to_bytes(cert) {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!("Failed to BCS-serialize certificate: {e}");
                    continue;
                }
            };

            tracing::info!(
                size = cert_bytes.len(),
                "Calling addBlock on FungibleBridge..."
            );

            let bridge_contract = IFungibleBridge::new(bridge_addr, &provider);
            match bridge_contract.addBlock(cert_bytes.into()).send().await {
                Ok(pending_tx) => match pending_tx.get_receipt().await {
                    Ok(receipt) => {
                        tracing::info!(
                            tx = ?receipt.transaction_hash,
                            "addBlock transaction confirmed"
                        );
                    }
                    Err(e) => tracing::error!("addBlock receipt failed: {e}"),
                },
                Err(e) => tracing::error!("addBlock send failed: {e}"),
            }
        }
    }

    Ok(())
}

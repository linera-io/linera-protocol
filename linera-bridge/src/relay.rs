// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Relay server for the EVM↔Linera bridge demo.
//!
//! Responsibilities:
//! - **HTTP**: `POST /deposit` — generates MPT deposit proofs and submits `ProcessDeposit`
//!   operations on the bridge chain.
//! - **Linera client**: claims a "bridge chain", listens for `NewIncomingBundle` notifications,
//!   processes the inbox, and burns any Address20 credits so the EVM contract can release tokens.
//! - **EVM forwarder**: after processing inbox and burns, BCS-serializes the resulting certificates
//!   and calls `FungibleBridge.addBlock(bytes)` on the EVM chain.

use std::{sync::Arc, time::Duration};

use alloy::{
    network::EthereumWallet, primitives::Address, providers::ProviderBuilder,
    signers::local::PrivateKeySigner, sol,
};
use anyhow::{Context as _, Result};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use futures::StreamExt as _;
use linera_base::{
    crypto::InMemorySigner,
    data_types::Amount,
    identifiers::{AccountOwner, ApplicationId},
};
use linera_chain::data_types::Transaction;
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{environment::wallet::Memory, worker::Reason};
use linera_execution::{Message, Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::DbStorage;
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use tokio::sync::{mpsc, oneshot};
use tower_http::cors::CorsLayer;

use crate::proof::gen::{DepositProofClient, HttpDepositProofClient, ProofError};

// ── Alloy ABI for FungibleBridge.addBlock ──

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
    }
}

// ── BCS-compatible type matching evm_bridge::BridgeOperation ──

/// Must match `evm_bridge::BridgeOperation` variant-for-variant for BCS compatibility.
#[derive(serde::Serialize)]
enum BridgeOperation {
    ProcessDeposit {
        block_header_rlp: Vec<u8>,
        receipt_rlp: Vec<u8>,
        proof_nodes: Vec<Vec<u8>>,
        tx_index: u64,
        log_index: u64,
    },
}

// ── Channel types for deposit requests ──

struct DepositRequest {
    proof: crate::proof::gen::DepositProof,
    response: oneshot::Sender<Result<(), String>>,
}

// ── Shared state for the HTTP server ──

struct AppState {
    proof_client: HttpDepositProofClient,
    deposit_tx: mpsc::Sender<DepositRequest>,
}

// ── HTTP handlers ──

#[derive(serde::Deserialize)]
struct DepositHttpRequest {
    tx_hash: String,
}

async fn deposit_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DepositHttpRequest>,
) -> impl IntoResponse {
    tracing::info!(tx_hash = %req.tx_hash, "Received deposit request");

    let tx_hash = match req.tx_hash.parse() {
        Ok(h) => h,
        Err(_) => {
            tracing::error!(tx_hash = %req.tx_hash, "Invalid tx_hash format");
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid tx_hash"})),
            );
        }
    };

    // Retry proof generation — on public testnets the RPC may not have
    // indexed the receipt yet when the frontend sends the tx hash.
    // Permanent errors (invalid tx, missing deposit event) fail immediately.
    tracing::info!(%tx_hash, "Generating deposit proof...");
    let mut proof = None;
    for attempt in 0..5 {
        match state.proof_client.generate_deposit_proof(tx_hash).await {
            Ok(p) => {
                tracing::info!(
                    %tx_hash,
                    tx_index = p.tx_index,
                    log_count = p.log_indices.len(),
                    "Deposit proof generated"
                );
                proof = Some(p);
                break;
            }
            Err(ProofError::Permanent(e)) => {
                tracing::error!(%tx_hash, "Deposit proof generation failed permanently: {e:#}");
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                );
            }
            Err(ProofError::Transient(e)) => {
                if attempt < 4 {
                    tracing::warn!(
                        %tx_hash, attempt, "Deposit proof generation failed, retrying: {e:#}"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(2 * (attempt + 1))).await;
                } else {
                    tracing::error!(%tx_hash, "Deposit proof generation failed after 5 attempts: {e:#}");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": format!("{e:#}")})),
                    );
                }
            }
        }
    }
    let proof = proof.unwrap();

    tracing::info!(%tx_hash, "Sending deposit to processing channel...");
    let (resp_tx, resp_rx) = oneshot::channel();
    if state
        .deposit_tx
        .send(DepositRequest {
            proof,
            response: resp_tx,
        })
        .await
        .is_err()
    {
        tracing::error!(%tx_hash, "Relay deposit channel closed");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "relay channel closed"})),
        );
    }

    match resp_rx.await {
        Ok(Ok(())) => {
            tracing::info!(%tx_hash, "Deposit processed successfully");
            (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
        }
        Ok(Err(e)) => {
            tracing::error!(%tx_hash, "Deposit processing failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
        Err(_) => {
            tracing::error!(%tx_hash, "Deposit response channel closed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "channel closed"})),
            )
        }
    }
}

// ── Helpers ──

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

/// Poll a file for the evm-bridge ApplicationId (hex-encoded BCS).
async fn resolve_bridge_app_id(file_path: &str) -> Result<ApplicationId> {
    tracing::info!(file = file_path, "Polling for bridge app ID...");
    loop {
        if let Ok(contents) = tokio::fs::read_to_string(file_path).await {
            let id_str = contents.trim();
            if !id_str.is_empty() {
                let app_id: ApplicationId =
                    id_str.parse().context("invalid ApplicationId in file")?;
                tracing::info!(%app_id, "Read bridge app ID from file");
                return Ok(app_id);
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Poll a file for the wrapped-fungible ApplicationId (hex-encoded BCS).
async fn resolve_fungible_app_id(file_path: &str) -> Result<ApplicationId> {
    tracing::info!(file = file_path, "Polling for wrapped-fungible app ID...");
    loop {
        if let Ok(contents) = tokio::fs::read_to_string(file_path).await {
            let id_str = contents.trim();
            if !id_str.is_empty() {
                let app_id: ApplicationId =
                    id_str.parse().context("invalid ApplicationId in file")?;
                tracing::info!(%app_id, "Read wrapped-fungible app ID from file");
                return Ok(app_id);
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Extract (owner, amount) from a fungible Credit message if the target is Address20.
///
/// BCS layout: variant 0 (Credit) + target: AccountOwner + amount: Amount + source: AccountOwner
fn try_parse_credit_to_address20(bytes: &[u8]) -> Option<(AccountOwner, Amount)> {
    // Variant 0 = Credit
    if bytes.first() != Some(&0) {
        return None;
    }
    #[derive(serde::Deserialize)]
    struct Credit {
        target: AccountOwner,
        amount: Amount,
        _source: AccountOwner,
    }
    let credit: Credit = bcs::from_bytes(&bytes[1..]).ok()?;
    if !matches!(credit.target, AccountOwner::Address20(_)) {
        return None;
    }
    Some((credit.target, credit.amount))
}

/// BCS-serialize a WrappedFungibleOperation::Burn (variant index 7).
fn serialize_burn_operation(owner: &AccountOwner, amount: &Amount) -> Vec<u8> {
    let mut buf = vec![7u8];
    buf.extend(bcs::to_bytes(owner).unwrap());
    buf.extend(bcs::to_bytes(amount).unwrap());
    buf
}

// ── EVM forwarding helper ──

/// BCS-serialize and forward a certified block to FungibleBridge on EVM.
async fn forward_cert_to_evm(
    cert: &impl serde::Serialize,
    bridge_addr: Address,
    provider: &impl alloy::providers::Provider,
) {
    let cert_bytes = match bcs::to_bytes(cert) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("Failed to BCS-serialize certificate: {e}");
            return;
        }
    };

    tracing::info!(
        size = cert_bytes.len(),
        "Calling addBlock on FungibleBridge..."
    );

    let bridge_contract = IFungibleBridge::new(bridge_addr, provider);
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

// ── Entry point ──

pub async fn run(
    rpc_url: &str,
    faucet_url: &str,
    bridge_address: Option<&str>,
    bridge_address_file: &str,
    bridge_app_id_file: &str,
    fungible_app_id_file: &str,
    evm_private_key: &str,
    port: u16,
) -> Result<()> {
    tracing_subscriber::fmt::init();

    // Tonic pulls in rustls 0.23 which requires an explicit crypto provider.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing::info!("Starting bridge relay server...");

    // ── 1. Set up Linera client ──
    tracing::info!("Connecting to Linera faucet at {faucet_url}...");
    let faucet = Faucet::new(faucet_url.to_string());
    tracing::info!("Fetching genesis config...");
    let genesis_config = faucet.genesis_config().await?;
    tracing::info!("Genesis config received");

    let config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    tracing::info!("Creating storage...");
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &config,
        "bridge-relay",
        Some(WasmRuntime::default()),
    )
    .await?;

    tracing::info!("Initializing storage from genesis...");
    genesis_config.initialize_storage(&mut storage).await?;
    tracing::info!("Storage initialized");

    let admin_chain_id = genesis_config.admin_chain_id();
    let mut signer = InMemorySigner::new(None);

    tracing::info!("Creating client context...");
    let mut ctx = ClientContext::new(
        storage,
        Memory::default(),
        signer.clone(),
        &Default::default(),
        None,
        genesis_config,
        10_000,
        10_000,
    )
    .await?;
    tracing::info!("Client context created");

    // ── 1b. Sync admin chain from validators ──
    tracing::info!(%admin_chain_id, "Syncing admin chain from validators...");
    let committee = faucet.current_committee().await?;
    tracing::info!(
        validators = committee.validators().into_iter().count(),
        "Fetched current committee, downloading chain state..."
    );
    let admin_client = ctx.make_chain_client(admin_chain_id).await?;
    admin_client
        .synchronize_chain_state_from_committee(committee)
        .await?;
    tracing::info!("Admin chain synced");

    // ── 2. Claim bridge chain ──
    tracing::info!("Claiming bridge chain from faucet...");
    let owner = AccountOwner::from(signer.generate_new());
    let chain_desc = faucet.claim(&owner).await?;
    let chain_id = chain_desc.id();
    tracing::info!(%chain_id, %owner, "Chain claimed, extending wallet...");
    ctx.extend_with_chain(chain_desc, Some(owner)).await?;

    tracing::info!("Synchronizing bridge chain from validators...");
    let chain_client = ctx.make_chain_client(chain_id).await?;
    chain_client.synchronize_from_validators().await?;

    tracing::info!(%chain_id, "Bridge chain claimed");

    // Write chain ID and owner to /shared/ if the directory exists (Docker mode).
    if let Ok(()) = fs_err::write("/shared/bridge-chain-id", chain_id.to_string()) {
        tracing::info!("Wrote bridge chain ID to /shared/bridge-chain-id");
    }
    if let Ok(()) = fs_err::write("/shared/relay-owner", owner.to_string()) {
        tracing::info!("Wrote relay owner to /shared/relay-owner");
    }

    // ── 3. Set up EVM provider ──
    let bridge_addr = resolve_bridge_address(bridge_address, bridge_address_file).await?;
    let evm_signer: PrivateKeySigner =
        evm_private_key.parse().context("invalid EVM private key")?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .with_simple_nonce_management()
        .connect_http(rpc_url.parse().context("invalid RPC URL")?);

    // ── 4. Resolve app IDs ──
    let bridge_app_id = resolve_bridge_app_id(bridge_app_id_file).await?;
    let fungible_app_id = resolve_fungible_app_id(fungible_app_id_file).await?;

    // ── 5. Start notification listener ──
    let mut notifications = chain_client.subscribe()?;
    let (listener, _abort_handle, _) = chain_client.listen().await?;
    tokio::spawn(listener);

    // ── 6. Start HTTP server ──
    let proof_client = HttpDepositProofClient::new(rpc_url)?;
    let (deposit_tx, mut deposit_rx) = mpsc::channel::<DepositRequest>(16);
    let app_state = Arc::new(AppState {
        proof_client,
        deposit_tx,
    });

    let app = Router::new()
        .route("/deposit", post(deposit_handler))
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

    // ── 7. Main loop: process notifications + deposit requests ──
    tracing::info!("Listening for notifications and deposit requests...");
    loop {
        tokio::select! {
            notification = notifications.next() => {
                let notification = match notification {
                    Some(n) => n,
                    None => {
                        tracing::warn!("Notification stream ended, exiting");
                        break;
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
                for cert in &certs {
                    forward_cert_to_evm(cert, bridge_addr, &provider).await;
                }

                // Scan inbox certs for Credit messages to Address20 and submit Burns.
                let mut burn_ops = vec![];
                for cert in &certs {
                    for txn in &cert.block().body.transactions {
                        if let Transaction::ReceiveMessages(bundle) = txn {
                            for posted in &bundle.bundle.messages {
                                if let Message::User { application_id, bytes } = &posted.message {
                                    if application_id == &fungible_app_id {
                                        if let Some((owner, amount)) = try_parse_credit_to_address20(bytes.as_slice()) {
                                            burn_ops.push(Operation::User {
                                                application_id: fungible_app_id,
                                                bytes: serialize_burn_operation(&owner, &amount),
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if !burn_ops.is_empty() {
                    tracing::info!(count = burn_ops.len(), "Submitting burn operations...");
                    if let Err(e) = chain_client.synchronize_from_validators().await {
                        tracing::error!("Failed to synchronize before burn: {e}");
                        continue;
                    }
                    match chain_client.execute_operations(burn_ops, vec![]).await {
                        Ok(linera_core::data_types::ClientOutcome::Committed(cert)) => {
                            tracing::info!(
                                height = %cert.block().header.height,
                                "Burn operations committed"
                            );
                            forward_cert_to_evm(&cert, bridge_addr, &provider).await;
                        }
                        Ok(other) => tracing::error!("Burn not committed: {other:?}"),
                        Err(e) => tracing::error!("Burn submission failed: {e}"),
                    }
                }
            }

            Some(deposit_req) = deposit_rx.recv() => {
                let result = async {
                    let proof = &deposit_req.proof;

                    let operations: Vec<_> = proof.log_indices.iter().map(|&log_index| {
                        let op = BridgeOperation::ProcessDeposit {
                            block_header_rlp: proof.block_header_rlp.clone(),
                            receipt_rlp: proof.receipt_rlp.clone(),
                            proof_nodes: proof.proof_nodes.clone(),
                            tx_index: proof.tx_index,
                            log_index,
                        };
                        let op_bytes = bcs::to_bytes(&op)
                            .expect("failed to BCS-serialize BridgeOperation");
                        Operation::User {
                            application_id: bridge_app_id,
                            bytes: op_bytes,
                        }
                    }).collect();

                    tracing::info!(
                        count = operations.len(),
                        "Submitting ProcessDeposit operations on bridge chain..."
                    );

                    chain_client.synchronize_from_validators().await
                        .context("failed to synchronize")?;

                    let outcome = chain_client
                        .execute_operations(operations, vec![])
                        .await?;
                    let cert = match outcome {
                        linera_core::data_types::ClientOutcome::Committed(cert) => {
                            tracing::info!(
                                height = %cert.block().header.height,
                                "ProcessDeposit committed"
                            );
                            cert
                        }
                        other => {
                            anyhow::bail!("ProcessDeposit not committed: {other:?}");
                        }
                    };

                    forward_cert_to_evm(&cert, bridge_addr, &provider).await;

                    Ok::<(), anyhow::Error>(())
                }.await;

                let _ = deposit_req.response.send(
                    result.map_err(|e| format!("{e:#}"))
                );
            }
        }
    }

    Ok(())
}

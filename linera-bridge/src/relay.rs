// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Relay server for the EVM↔Linera bridge demo.
//!
//! Responsibilities:
//! - **HTTP**: `POST /deposit` — generates MPT deposit proofs and submits `ProcessDeposit`
//!   operations on the bridge chain.
//! - **Linera client**: manages a "bridge chain", listens for `NewIncomingBundle` notifications,
//!   processes the inbox, and burns any Address20 credits so the EVM contract can release tokens.
//! - **EVM forwarder**: after processing inbox and burns, BCS-serializes the resulting certificates
//!   and calls `FungibleBridge.addBlock(bytes)` on the EVM chain.

use std::{path::Path, sync::Arc};

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
    identifiers::{AccountOwner, ApplicationId, ChainId},
};
use linera_chain::data_types::Transaction;
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{client::ChainClient, worker::Reason};
use linera_execution::{Message, Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_persistent::Persist;
use linera_storage::DbStorage;
use linera_views::{
    backends::{
        lru_caching::LruCachingConfig,
        rocks_db::{PathWithGuard, RocksDbDatabase, RocksDbSpawnMode, RocksDbStoreInternalConfig},
    },
    lru_prefix_cache::StorageCacheConfig,
};
use linera_wallet_json::PersistentWallet;
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

// ── RocksDB storage helper ──

type RocksDbStorage = DbStorage<RocksDbDatabase, linera_storage::WallClock>;

async fn create_rocksdb_storage(path: &Path, blob_cache_size: usize) -> Result<RocksDbStorage> {
    let config = LruCachingConfig {
        inner_config: RocksDbStoreInternalConfig {
            path_with_guard: PathWithGuard::new(path.to_path_buf()),
            spawn_mode: RocksDbSpawnMode::get_spawn_mode_from_runtime(),
            max_stream_queries: 10,
        },
        storage_cache_config: StorageCacheConfig {
            max_cache_size: 10_000_000,
            max_value_entry_size: 1_000_000,
            max_find_keys_entry_size: 10_000_000,
            max_find_key_values_entry_size: 10_000_000,
            max_cache_entries: 1000,
            max_cache_value_size: 10_000_000,
            max_cache_find_keys_size: 10_000_000,
            max_cache_find_key_values_size: 10_000_000,
        },
    };
    let storage = DbStorage::<RocksDbDatabase, _>::maybe_create_and_connect(
        &config,
        "bridge_relay",
        Some(WasmRuntime::default()),
        linera_storage::StorageCacheConfig {
            blob_cache_size,
            confirmed_block_cache_size: blob_cache_size,
            certificate_cache_size: blob_cache_size,
            certificate_raw_cache_size: blob_cache_size,
            event_cache_size: blob_cache_size,
            cache_cleanup_interval_secs: linera_storage::DEFAULT_CLEANUP_INTERVAL_SECS,
        },
    )
    .await?;
    Ok(storage)
}

// ── Entry point ──

#[allow(clippy::too_many_arguments)]
pub async fn run(
    rpc_url: &str,
    faucet_url: &str,
    wallet_path: Option<&Path>,
    keystore_path: Option<&Path>,
    storage_config: Option<&str>,
    chain_id_arg: Option<ChainId>,
    evm_bridge_address: &str,
    linera_bridge_address: &str,
    linera_fungible_address: &str,
    evm_private_key: &str,
    port: u16,
    blob_cache_size: usize,
) -> Result<()> {
    tracing_subscriber::fmt::init();

    // Tonic pulls in rustls 0.23 which requires an explicit crypto provider.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing::info!("Starting bridge relay server...");

    // ── Resolve paths (same defaults as linera binary: ~/.config/linera/) ──
    let default_dir = dirs::config_dir()
        .context("no config directory on this platform")?
        .join("linera");
    let wallet_path = wallet_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| default_dir.join("wallet.json"));
    let keystore_path = keystore_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| default_dir.join("keystore.json"));
    let storage_path = storage_config
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("rocksdb:{}", default_dir.join("wallet.db").display()));

    tracing::info!(
        wallet = %wallet_path.display(),
        keystore = %keystore_path.display(),
        storage = %storage_path,
        "Resolved paths"
    );

    // ── Common init ──
    tracing::info!("Connecting to Linera faucet at {faucet_url}...");
    let faucet = Faucet::new(faucet_url.to_string());
    let genesis_config = faucet.genesis_config().await?;
    tracing::info!("Genesis config received");

    let mut signer: InMemorySigner =
        linera_persistent::File::<InMemorySigner>::read(&keystore_path)
            .context("failed to read keystore")?
            .into_value();

    // Parse storage path: expect "rocksdb:/path/to/db"
    let db_path = storage_path
        .strip_prefix("rocksdb:")
        .context("storage config must start with 'rocksdb:'")?;
    let mut storage = create_rocksdb_storage(Path::new(db_path), blob_cache_size).await?;

    // ── Wallet: load existing or create fresh ──
    let wallet_exists = wallet_path.exists();

    // Always initialize storage — this is a no-op if already initialized.
    genesis_config.initialize_storage(&mut storage).await?;

    let wallet = if wallet_exists {
        tracing::info!("Loading existing wallet from {}", wallet_path.display());
        PersistentWallet::read(&wallet_path).context("failed to read wallet")?
    } else {
        tracing::info!("Creating new wallet at {}", wallet_path.display());
        PersistentWallet::create(&wallet_path, genesis_config).context("failed to create wallet")?
    };

    let admin_chain_id = wallet.genesis_config().admin_chain_id();
    let genesis_config = wallet.genesis_config().clone();
    let mut ctx = ClientContext::new(
        storage,
        wallet,
        signer.clone(),
        &Default::default(),
        None,
        genesis_config,
        linera_core::worker::DEFAULT_BLOCK_CACHE_SIZE,
        linera_core::worker::DEFAULT_EXECUTION_STATE_CACHE_SIZE,
    )
    .await?;

    // ── Sync admin chain (always) ──
    tracing::info!(%admin_chain_id, "Syncing admin chain from validators...");
    let committee = faucet.current_committee().await?;
    tracing::info!(
        validators = committee.validators().len(),
        "Fetched current committee, downloading chain state..."
    );
    let admin_client = ctx.make_chain_client(admin_chain_id).await?;
    admin_client
        .synchronize_chain_state_from_committee(committee)
        .await?;
    tracing::info!("Admin chain synced");

    // ── Resolve bridge chain ──
    let (chain_id, _owner) = if let Some(cid) = chain_id_arg {
        // Register in wallet if not already there.
        if ctx.wallet().get(cid).is_none() {
            let key_owner = signer.keys().first().context("keystore has no keys")?.0;
            ctx.update_wallet_for_new_chain(
                cid,
                Some(key_owner),
                linera_base::data_types::Timestamp::default(),
                linera_base::data_types::Epoch::ZERO,
            )
            .await?;
        }

        // Sync from validators.
        let chain_client = ctx.make_chain_client(cid).await?;
        chain_client.synchronize_from_validators().await?;

        // Verify our keystore contains an owner key for this chain.
        let ownership = chain_client.query_chain_ownership().await?;
        let our_keys: Vec<AccountOwner> = signer.keys().into_iter().map(|(o, _)| o).collect();
        let owner = our_keys
            .into_iter()
            .find(|o| ownership.super_owners.contains(o) || ownership.owners.contains_key(o))
            .context("keystore has no key that is an owner of the specified --chain-id")?;
        tracing::info!(%cid, %owner, "Using pre-existing chain");
        (cid, owner)
    } else {
        // Claim from faucet.
        tracing::info!("Claiming bridge chain from faucet...");
        let owner = AccountOwner::from(signer.generate_new());
        let chain_desc = faucet.claim(&owner).await?;
        let cid = chain_desc.id();
        tracing::info!(%cid, %owner, "Chain claimed, extending wallet...");
        ctx.extend_with_chain(chain_desc, Some(owner)).await?;

        // Save updated keystore (has new key from generate_new).
        let mut ks_file = linera_persistent::File::new(&keystore_path, signer.clone())?;
        ks_file.persist().await?;

        // Sync bridge chain.
        let chain_client = ctx.make_chain_client(cid).await?;
        chain_client.synchronize_from_validators().await?;
        tracing::info!(%cid, "Bridge chain claimed and synced");
        (cid, owner)
    };

    let chain_client = ctx.make_chain_client(chain_id).await?;

    Box::pin(serve_loop(
        chain_client,
        rpc_url,
        evm_bridge_address,
        linera_bridge_address,
        linera_fungible_address,
        evm_private_key,
        port,
    ))
    .await
}

// ── Main event loop ──

#[allow(clippy::too_many_arguments)]
async fn serve_loop<E: linera_core::environment::Environment>(
    chain_client: ChainClient<E>,
    rpc_url: &str,
    evm_bridge_address: &str,
    linera_bridge_address: &str,
    linera_fungible_address: &str,
    evm_private_key: &str,
    port: u16,
) -> Result<()> {
    // ── Set up EVM provider ──
    let bridge_addr: Address = evm_bridge_address
        .parse()
        .context("invalid --evm-bridge-address")?;
    let evm_signer: PrivateKeySigner =
        evm_private_key.parse().context("invalid EVM private key")?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .with_simple_nonce_management()
        .connect_http(rpc_url.parse().context("invalid RPC URL")?);

    // ── Parse app IDs ──
    let bridge_app_id: ApplicationId = linera_bridge_address
        .parse()
        .context("invalid --linera-bridge-address")?;
    let fungible_app_id: ApplicationId = linera_fungible_address
        .parse()
        .context("invalid --linera-fungible-address")?;

    // ── Start notification listener ──
    let mut notifications = chain_client.subscribe()?;
    let (listener, _abort_handle, _) = chain_client.listen().await?;
    tokio::spawn(listener);

    // ── Start HTTP server ──
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

    tracing::info!(
        %bridge_addr,
        %bridge_app_id,
        %fungible_app_id,
        "Relay is ready"
    );

    // ── Main loop: process notifications + deposit requests ──
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

                let certs = match Box::pin(chain_client.process_inbox()).await {
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

                    // Forward the deposit block to EVM so the Microchain
                    // height stays sequential.
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

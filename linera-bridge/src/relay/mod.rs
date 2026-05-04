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

use linera_base::crypto::Signer as _;

mod committee;
pub mod evm;
pub mod linera;
pub(crate) mod metrics;

use std::{path::Path, sync::Arc, time::Duration};

use alloy::{
    network::EthereumWallet, primitives::Address, providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
};
use anyhow::{Context as _, Result};
use futures::StreamExt as _;
use linera_base::identifiers::{AccountOwner, ApplicationId, ChainId};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{client::ChainClient, worker::Reason};
use linera_execution::{Operation, WasmRuntime};
use linera_storage::{DbStorage, Storage as _};
use linera_storage_runtime::{CommonStorageOptions, StorageConfig, StoreConfig};
use linera_views::backends::rocks_db::RocksDbDatabase;
use linera_wallet_json::PersistentWallet;
use tokio::sync::{mpsc, Notify, RwLock};

use crate::{
    monitor::{self, MonitorState},
    proof::gen::HttpDepositProofClient,
};

/// Queries both chain balances and updates the prometheus metrics.
pub(crate) async fn update_balance_metrics<
    P: alloy::providers::Provider,
    E: linera_core::environment::Environment,
>(
    evm_client: &evm::EvmClient<P>,
    linera_client: &linera::LineraClient<E>,
) {
    update_evm_balance_metric(evm_client).await;
    update_linera_balance_metric(linera_client).await;
}

pub(crate) async fn update_evm_balance_metric<P: alloy::providers::Provider>(
    evm_client: &evm::EvmClient<P>,
) {
    match evm_client.get_relayer_balance().await {
        Ok(balance) => {
            // U256::to::<u128> panics if >u128::MAX, but ETH supply fits in u128.
            let wei: u128 = balance.to();
            metrics::set_relayer_evm_balance(wei as f64);
        }
        Err(e) => tracing::warn!("Failed to query EVM relayer balance: {e:#}"),
    }
}

pub(crate) async fn update_linera_balance_metric<E: linera_core::environment::Environment>(
    linera_client: &linera::LineraClient<E>,
) {
    match linera_client.chain_balance().await {
        Ok(balance) => metrics::set_relayer_linera_balance(u128::from(balance) as f64),
        Err(e) => tracing::warn!("Failed to query Linera chain balance: {e:#}"),
    }
}

#[expect(clippy::too_many_arguments)]
pub async fn run(
    rpc_url: &str,
    wallet_path: Option<&Path>,
    keystore_path: Option<&Path>,
    storage_config: Option<&str>,
    chain_id: ChainId,
    chain_owner: AccountOwner,
    evm_bridge_address: &str,
    linera_bridge_address: &str,
    linera_fungible_address: &str,
    evm_private_key: &str,
    evm_light_client_address: Option<&str>,
    port: u16,
    common_storage_options: &CommonStorageOptions,
    monitor_scan_interval: Duration,
    monitor_start_block: u64,
    max_retries: u32,
    sqlite_path: Option<&Path>,
) -> Result<()> {
    tracing::info!("Starting bridge relay server...");

    // ── Resolve paths (same defaults as linera binary: ~/.config/linera/) ──
    let default_dir = dirs::config_dir()
        .context("no config directory on this platform")?
        .join("linera");
    let wallet_path =
        wallet_path.map_or_else(|| default_dir.join("wallet.json"), |p| p.to_path_buf());
    let keystore_path =
        keystore_path.map_or_else(|| default_dir.join("keystore.json"), |p| p.to_path_buf());
    let storage_path = storage_config.map_or_else(
        || format!("rocksdb:{}", default_dir.join("wallet.db").display()),
        |s| s.to_string(),
    );

    tracing::info!(
        wallet = %wallet_path.display(),
        keystore = %keystore_path.display(),
        storage = %storage_path,
        "Resolved paths"
    );

    anyhow::ensure!(
        wallet_path.exists(),
        "wallet not found at {}",
        wallet_path.display(),
    );

    let keystore =
        linera_wallet_json::Keystore::read(&keystore_path).context("failed to read keystore")?;
    let signer = keystore.into_signer();

    // Parse storage config and create storage.
    let mut storage_config: StorageConfig = storage_path.parse()?;
    storage_config.namespace = "bridge_relay".to_string();
    let store_config = storage_config.add_common_storage_options(common_storage_options)?;
    let cache_sizes = common_storage_options.storage_cache_sizes();
    let (mut storage, db_path) = match store_config {
        StoreConfig::RocksDb { config, namespace } => {
            let path = config.inner_config.path_with_guard.path_buf.clone();
            let storage = DbStorage::<RocksDbDatabase, _>::maybe_create_and_connect(
                &config,
                &namespace,
                Some(WasmRuntime::default()),
                cache_sizes,
            )
            .await?;
            (storage, path)
        }
        _ => anyhow::bail!("only rocksdb storage is supported by the bridge relay"),
    };

    // Wallet: must already exist
    tracing::info!(path = %wallet_path.display(), "Loading existing wallet");
    let wallet = PersistentWallet::read(&wallet_path).context("failed to read wallet")?;
    wallet
        .genesis_config()
        .initialize_storage(&mut storage)
        .await?;

    let admin_chain_id = wallet.genesis_config().admin_chain_id();
    let genesis_config = wallet.genesis_config().clone();
    let mut ctx = ClientContext::new(
        storage,
        wallet,
        signer.clone(),
        &Default::default(),
        None,
        genesis_config,
        common_storage_options.block_cache_size,
        common_storage_options.execution_state_cache_size,
    )
    .await?;

    // ── Sync admin chain ──
    tracing::info!(%admin_chain_id, "Syncing admin chain from validators...");
    let admin_client = ctx.make_chain_client(admin_chain_id).await?;
    admin_client.synchronize_from_validators().await?;
    let admin_chain_height = admin_client.chain_info().await?.next_block_height;
    tracing::info!(%admin_chain_height, "Admin chain synced");

    // ── Register bridge chain in the local wallet ──
    anyhow::ensure!(
        signer
            .contains_key(&chain_owner)
            .await
            .context("failed to query keystore")?,
        "keystore does not contain a key for owner {chain_owner}"
    );

    ctx.update_wallet_for_new_chain(
        chain_id,
        Some(chain_owner),
        linera_base::data_types::Timestamp::default(),
        linera_base::data_types::Epoch::ZERO,
    )
    .await?;

    ctx.client
        .extend_chain_mode(chain_id, linera_core::client::ListeningMode::FullChain);

    let chain_client = ctx.make_chain_client(chain_id).await?;
    chain_client.synchronize_from_validators().await?;
    tracing::info!(%chain_id, %chain_owner, "Bridge chain registered and synced");

    Box::pin(serve_loop(
        chain_client,
        rpc_url,
        evm_bridge_address,
        linera_bridge_address,
        linera_fungible_address,
        evm_private_key,
        evm_light_client_address,
        port,
        monitor_scan_interval,
        monitor_start_block,
        max_retries,
        sqlite_path,
        &db_path,
        admin_chain_id,
        admin_chain_height,
    ))
    .await
}

#[expect(clippy::too_many_arguments)]
async fn serve_loop<E: linera_core::environment::Environment + 'static>(
    chain_client: ChainClient<E>,
    rpc_url: &str,
    evm_bridge_address: &str,
    linera_bridge_address: &str,
    linera_fungible_address: &str,
    evm_private_key: &str,
    evm_light_client_address: Option<&str>,
    port: u16,
    monitor_scan_interval: Duration,
    monitor_start_block: u64,
    max_retries: u32,
    sqlite_path_override: Option<&Path>,
    storage_dir: &Path,
    admin_chain_id: ChainId,
    admin_chain_height: linera_base::data_types::BlockHeight,
) -> Result<()> {
    // ── Set up centralized clients ──
    let bridge_addr: Address = evm_bridge_address
        .parse()
        .context("invalid --evm-bridge-address")?;
    let evm_signer: PrivateKeySigner =
        evm_private_key.parse().context("invalid EVM private key")?;
    let relayer_addr = evm_signer.address();
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .with_simple_nonce_management()
        .connect_http(rpc_url.parse().context("invalid RPC URL")?);

    let light_client_addr: Option<Address> = evm_light_client_address
        .map(|s| s.parse())
        .transpose()
        .context("invalid --evm-light-client-address")?;
    let evm_client = Arc::new(evm::EvmClient::new(
        provider,
        bridge_addr,
        relayer_addr,
        light_client_addr,
    ));

    // ── Catch up LightClient with any missed committee rotations ──
    committee::catch_up(
        chain_client.storage_client(),
        &evm_client,
        admin_chain_id,
        admin_chain_height,
    )
    .await
    .context("committee catch-up failed")?;

    let bridge_app_id: ApplicationId = linera_bridge_address
        .parse()
        .context("invalid --linera-bridge-address")?;
    let fungible_app_id: ApplicationId = linera_fungible_address
        .parse()
        .context("invalid --linera-fungible-address")?;

    let (op_tx, mut op_rx) = mpsc::channel::<linera::ChainOperation>(16);
    let linera_client = Arc::new(linera::LineraClient::new(
        chain_client.clone(),
        op_tx,
        bridge_app_id,
        fungible_app_id,
    ));

    // ── Start notification listener ──
    // Subscribe to admin chain notifications so we detect committee updates
    // when synchronize_publisher_chains downloads new admin chain blocks.
    let chain_notifications = chain_client.subscribe()?;
    let admin_notifications = chain_client.subscribe_to(admin_chain_id)?;
    let mut notifications = futures::stream::select(chain_notifications, admin_notifications);
    let (listener, _abort_handle, _) = chain_client.listen().await?;
    let mut chain_listener_handle = tokio::spawn(listener);

    // ── Monitor state + scan/retry ──
    let mut monitor_state = MonitorState::new(monitor_start_block);
    let default_sqlite_path = storage_dir
        .parent()
        .unwrap_or(storage_dir)
        .join("bridge_relay.sqlite3");
    let sqlite_path = sqlite_path_override.unwrap_or(&default_sqlite_path);
    let db = monitor::db::BridgeDb::open(sqlite_path)
        .await
        .with_context(|| {
            format!(
                "failed to open SQLite database at {}",
                sqlite_path.display()
            )
        })?;
    tracing::info!(path = %sqlite_path.display(), "Opened bridge relay SQLite database");
    monitor_state.set_db(db);
    monitor_state
        .load_from_db()
        .await
        .context("failed to recover pending bridge requests from SQLite WAL")?;
    let monitor = Arc::new(RwLock::new(monitor_state));
    let deposit_notify = Arc::new(Notify::new());
    let burn_notify = Arc::new(Notify::new());

    let mut evm_scan_handle = {
        let monitor = Arc::clone(&monitor);
        let evm_client = Arc::clone(&evm_client);
        let linera_client = Arc::clone(&linera_client);
        let deposit_notify = Arc::clone(&deposit_notify);
        tokio::spawn(monitor::evm::evm_scan_loop(
            monitor,
            evm_client,
            linera_client,
            deposit_notify,
            monitor_scan_interval,
        ))
    };
    let mut linera_scan_handle = {
        let monitor = Arc::clone(&monitor);
        let evm_client = Arc::clone(&evm_client);
        let linera_client = Arc::clone(&linera_client);
        let burn_notify = Arc::clone(&burn_notify);
        tokio::spawn(monitor::linera::linera_scan_loop(
            monitor,
            evm_client,
            linera_client,
            burn_notify,
            monitor_scan_interval,
        ))
    };

    let mut retry_handle = {
        let monitor = Arc::clone(&monitor);
        let evm_client = Arc::clone(&evm_client);
        let linera_client = Arc::clone(&linera_client);
        let proof_client = HttpDepositProofClient::new(rpc_url)?;
        tokio::spawn(monitor::retry_loop(
            monitor,
            proof_client,
            evm_client,
            linera_client,
            deposit_notify,
            burn_notify,
            monitor_scan_interval,
            max_retries,
        ))
    };

    let app = metrics::build_router();

    let bind_addr = format!("0.0.0.0:{port}");
    tracing::info!("HTTP server listening on {bind_addr}");

    let tcp_listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    let mut http_server_handle = tokio::spawn(async move {
        axum::serve(tcp_listener, app)
            .await
            .context("HTTP server error")
    });

    update_balance_metrics(&evm_client, &linera_client).await;

    tracing::info!(
        %bridge_addr,
        %bridge_app_id,
        %fungible_app_id,
        "Relay is ready"
    );

    // ── Main loop: process chain operations + notifications ──
    tracing::info!("Listening for chain operations and notifications...");
    loop {
        tokio::select! {
            result = &mut chain_listener_handle => {
                anyhow::bail!("Chain listener exited unexpectedly: {result:?}");
            }
            result = &mut evm_scan_handle => {
                anyhow::bail!("EVM scan loop exited unexpectedly: {result:?}");
            }
            result = &mut linera_scan_handle => {
                anyhow::bail!("Linera scan loop exited unexpectedly: {result:?}");
            }
            result = &mut retry_handle => {
                anyhow::bail!("Retry loop exited unexpectedly: {result:?}");
            }
            result = &mut http_server_handle => {
                anyhow::bail!("HTTP server exited unexpectedly: {result:?}");
            }
            notification = notifications.next() => {
                let notification = match notification {
                    Some(n) => n,
                    None => {
                        tracing::warn!("Notification stream ended, exiting");
                        break;
                    }
                };

                // Handle admin chain committee updates.
                if notification.chain_id == admin_chain_id {
                    if let Reason::NewBlock { height, .. } = &notification.reason {
                        tracing::debug!(%height, "New admin chain block, checking for committee update");
                        let heights = vec![*height];
                        if let Ok(certs) = chain_client
                            .storage_client()
                            .read_certificates_by_heights(admin_chain_id, &heights)
                            .await
                        {
                            for cert in certs.into_iter().flatten() {
                                if let Some((epoch, blob_hash)) = committee::find_create_committee(&cert) {
                                    let blob_id = linera_base::identifiers::BlobId::new(
                                        blob_hash,
                                        linera_base::identifiers::BlobType::Committee,
                                    );
                                    match chain_client.storage_client().read_blob(blob_id).await {
                                        Ok(Some(blob)) => {
                                            match committee::relay_committee(&evm_client, &cert, blob.bytes()).await {
                                                Ok(()) => tracing::info!(?epoch, "Committee update relayed"),
                                                Err(e) => tracing::error!(?epoch, error = %e, "Failed to relay committee"),
                                            }
                                        }
                                        Ok(None) => tracing::error!(?blob_id, "Committee blob not found"),
                                        Err(e) => tracing::error!(error = %e, "Failed to read committee blob"),
                                    }
                                }
                            }
                        }
                    }
                    continue;
                }

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
                    tracing::debug!("No certificates from inbox processing");
                    continue;
                }

                tracing::info!(count = certs.len(), "Processed inbox certificates");
            }

            Some(op) = op_rx.recv() => {
                match op {
                    linera::ChainOperation::ProcessInbox { response } => {
                        let result = async {
                            chain_client.synchronize_from_validators().await
                                .context("failed to synchronize")?;
                            let (certs, _) = chain_client.process_inbox().await?;
                            Ok(certs)
                        }.await;
                        if response.send(result).is_err() {
                            tracing::debug!("ProcessInbox response receiver dropped");
                        }
                    }
                    linera::ChainOperation::ProcessDeposit { proof, response } => {
                        let result = async {
                            let operations: Vec<_> = proof.log_indices.iter().map(|&log_index| {
                                let op = crate::abi::BridgeOperation::ProcessDeposit {
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
                                "Submitting ProcessDeposit operations..."
                            );

                            chain_client.synchronize_from_validators().await
                                .context("failed to synchronize")?;

                            let outcome = chain_client
                                .execute_operations(operations, vec![])
                                .await?;
                            match outcome {
                                linera_core::data_types::ClientOutcome::Committed(cert) => {
                                    tracing::info!(
                                        height = %cert.block().header.height,
                                        "ProcessDeposit committed"
                                    );
                                }
                                other => {
                                    anyhow::bail!("ProcessDeposit not committed: {other:?}");
                                }
                            };
                            Ok(())
                        }.await;
                        if response.send(result).is_err() {
                            tracing::debug!("ProcessDeposit response receiver dropped");
                        }
                        update_balance_metrics(&evm_client, &linera_client).await;
                    }
                }
            }
        }
    }

    Ok(())
}

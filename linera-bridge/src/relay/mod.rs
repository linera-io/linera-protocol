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

pub mod evm;
mod http;
pub mod linera;

use std::{path::Path, sync::Arc, time::Duration};

use alloy::{
    network::EthereumWallet, primitives::Address, providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
};
use anyhow::{Context as _, Result};
use futures::StreamExt as _;
use linera_base::{
    crypto::InMemorySigner,
    identifiers::{AccountOwner, ApplicationId, ChainId},
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{client::ChainClient, wallet::PersistentWallet, worker::Reason};
use linera_execution::{Operation, WasmRuntime};
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
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::{
    monitor::{self, MonitorState},
    proof::gen::HttpDepositProofClient,
};

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
    cache_sizes: linera_storage::StorageCacheSizes,
    monitor_scan_interval: u64,
    monitor_start_block: u64,
    max_retries: u32,
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
    let mut storage = create_rocksdb_storage(Path::new(db_path), cache_sizes).await?;

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
        validators = committee.validators().iter().count(),
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

        // Register for notifications so the listener can connect to validators.
        ctx.client
            .extend_chain_mode(cid, linera_core::client::ListeningMode::FullChain);

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
        monitor_scan_interval,
        monitor_start_block,
        max_retries,
    ))
    .await
}

pub(crate) struct DepositRequest {
    pub(crate) proof: crate::proof::gen::DepositProof,
    pub(crate) response: oneshot::Sender<Result<(), String>>,
}

type RocksDbStorage = DbStorage<RocksDbDatabase, linera_storage::WallClock>;

async fn create_rocksdb_storage(
    path: &Path,
    cache_sizes: linera_storage::StorageCacheSizes,
) -> Result<RocksDbStorage> {
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
        cache_sizes,
    )
    .await?;
    Ok(storage)
}

#[allow(clippy::too_many_arguments)]
async fn serve_loop<E: linera_core::environment::Environment + 'static>(
    chain_client: ChainClient<E>,
    rpc_url: &str,
    evm_bridge_address: &str,
    linera_bridge_address: &str,
    linera_fungible_address: &str,
    evm_private_key: &str,
    port: u16,
    monitor_scan_interval: u64,
    monitor_start_block: u64,
    max_retries: u32,
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
    let chain_listener_handle = tokio::spawn(listener);

    // ── Monitor state + scan/retry channels ──
    let monitor = Arc::new(RwLock::new(MonitorState::new(monitor_start_block)));
    let scan_interval = Duration::from_secs(monitor_scan_interval);
    let (pending_deposit_tx, pending_deposit_rx) =
        tokio::sync::mpsc::channel::<monitor::PendingDeposit>(64);
    let (pending_burn_tx, pending_burn_rx) = tokio::sync::mpsc::channel::<monitor::PendingBurn>(64);

    let evm_scan_handle = {
        let monitor = Arc::clone(&monitor);
        let provider = provider.clone();
        let chain_client = chain_client.clone();
        tokio::spawn(monitor::evm::evm_scan_loop(
            monitor,
            provider,
            bridge_addr,
            chain_client,
            bridge_app_id,
            pending_deposit_tx,
            scan_interval,
        ))
    };
    let linera_scan_handle = {
        let monitor = Arc::clone(&monitor);
        let chain_client = chain_client.clone();
        let provider = provider.clone();
        tokio::spawn(monitor::linera::linera_scan_loop(
            monitor,
            chain_client,
            fungible_app_id,
            provider,
            bridge_addr,
            pending_burn_tx,
            scan_interval,
        ))
    };

    // ── Start HTTP server + retry loop ──
    let (deposit_tx, mut deposit_rx) = mpsc::channel::<DepositRequest>(16);

    let retry_handle = {
        let monitor = Arc::clone(&monitor);
        let deposit_tx = deposit_tx.clone();
        let proof_client = HttpDepositProofClient::new(rpc_url)?;
        let chain_client = chain_client.clone();
        let provider = provider.clone();
        tokio::spawn(monitor::retry_loop(
            monitor,
            deposit_tx,
            proof_client,
            pending_deposit_rx,
            chain_client,
            fungible_app_id,
            bridge_addr,
            provider,
            pending_burn_rx,
            max_retries,
        ))
    };

    let proof_client = HttpDepositProofClient::new(rpc_url)?;
    let app = http::build_router(proof_client, deposit_tx, Arc::clone(&monitor));

    let bind_addr = format!("0.0.0.0:{port}");
    tracing::info!("HTTP server listening on {bind_addr}");

    let tcp_listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    let http_server_handle = tokio::spawn(async move {
        axum::serve(tcp_listener, app)
            .await
            .context("HTTP server error")
    });

    tracing::info!(
        %bridge_addr,
        %bridge_app_id,
        %fungible_app_id,
        "Relay is ready"
    );

    // ── Main loop: process notifications + deposit requests ──
    tracing::info!("Listening for notifications and deposit requests...");
    let mut chain_listener_handle = chain_listener_handle;
    let mut evm_scan_handle = evm_scan_handle;
    let mut linera_scan_handle = linera_scan_handle;
    let mut retry_handle = retry_handle;
    let mut http_server_handle = http_server_handle;
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

                // Scan inbox certs for Credit messages to Address20 and submit Burns.
                let mut burn_ops = vec![];
                for cert in &certs {
                    for (owner, amount) in linera::find_address20_credits(
                        &cert.block().body.transactions,
                        fungible_app_id,
                    ) {
                        burn_ops.push(Operation::User {
                            application_id: fungible_app_id,
                            bytes: linera::serialize_burn_operation(&owner, &amount),
                        });
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
                            if let Err(e) = evm::forward_cert_to_evm(
                                &cert, bridge_addr, &provider,
                            ).await {
                                tracing::error!(
                                    "Failed to forward burn cert to EVM: {e:#}"
                                );
                            }
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
                        let op = evm::BridgeOperation::ProcessDeposit {
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

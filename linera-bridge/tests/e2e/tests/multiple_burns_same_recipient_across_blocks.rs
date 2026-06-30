// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Verifies that the relayer decides burn completion per burn rather
//! than per recipient: many burns to the same EVM recipient, spread
//! across separate Linera blocks, must each be released on-chain
//! before the relayer marks them complete.
//!
//! Setup: 5 burns to the same EVM recipient, materialised as 5
//! separate Linera blocks on the bridge chain BEFORE the relayer is
//! spawned. Each iteration submits one `BridgeOperation::Burn` to the
//! evm-bridge app on the user chain — the app escrows + burns the
//! wrapped tokens and routes a tracked `BridgeMessage::Burn` to the
//! bridge chain (chain A) — and the test process then drives chain
//! A's inbox once, so each `BurnEvent` lands in its own block
//! deterministically (no race with the relayer's notification loop
//! batching multiple events into one block). When the relayer
//! starts, its first scan iteration finds all 5 pending burns at once.
//!
//! The relayer must settle every burn via `registerBlock` + `processBurns` and the
//! per-burn `isBurnProcessed(height, eventIndex)` query must return
//! true for every entry; the recipient's ERC-20 balance must equal
//! `5 * amount`.

#![recursion_limit = "512"]

use std::time::Duration;

use alloy::{primitives::U256, providers::ProviderBuilder};
use linera_base::{crypto::InMemorySigner, data_types::U128, identifiers::AccountOwner};
use linera_bridge::{abi::BridgeOperation, contracts::IERC20};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token, fund_bridge_erc20,
    light_client_address, parse_metric_value, publish_and_create_evm_bridge,
    publish_and_create_wrapped_fungible, register_bridge_app, start_compose, wait_for_light_client,
    wait_for_relay_http_ready, wait_for_relay_metrics, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::DbStorage;
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};

const NUM_BURNS: u32 = 5;
const BURN_AMOUNT_TOKENS: u128 = 1;

#[tokio::test]
#[ignore] // Requires pre-built docker images, Wasm, and relay binary.
async fn relayer_processes_every_burn_to_same_recipient() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-multi-burn-same-recipient-test";

    let compose = start_compose(&compose_file, project_name).await;
    wait_for_light_client(&compose, project_name, &compose_file).await;

    let faucet = Faucet::new("http://localhost:8080".to_string());
    let genesis_config = faucet.genesis_config().await?;
    let relay_genesis_config = genesis_config.clone();

    let store_config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &store_config,
        "multi-burn-same-recipient-e2e",
        Some(WasmRuntime::default()),
        linera_bridge_e2e::test_storage_cache_config(),
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
        linera_core::worker::DEFAULT_BLOCK_CACHE_SIZE,
        linera_core::worker::DEFAULT_EXECUTION_STATE_CACHE_SIZE,
    )
    .await?;

    // Chain A hosts the bridge contract
    let owner_a = AccountOwner::from(signer.generate_new());
    let chain_a_desc = faucet.claim(&owner_a).await?;
    let chain_a = chain_a_desc.id();
    ctx.extend_with_chain(chain_a_desc, Some(owner_a)).await?;
    let cc_a = ctx.make_chain_client(chain_a).await?;
    cc_a.synchronize_from_validators().await?;

    // Chain B is the user.
    let owner_b = AccountOwner::from(signer.generate_new());
    let chain_b_desc = faucet.claim(&owner_b).await?;
    let chain_b = chain_b_desc.id();
    ctx.extend_with_chain(chain_b_desc, Some(owner_b)).await?;
    let cc_b = ctx.make_chain_client(chain_b).await?;
    cc_b.synchronize_from_validators().await?;

    let erc20_addr = deploy_linera_token(&compose, project_name, &compose_file).await?;

    let fungible_app_id = publish_and_create_wrapped_fungible(
        &cc_b,
        owner_b,
        chain_a,
        erc20_addr,
        1_000 * 10u128.pow(18),
    )
    .await?;

    let bridge_app_id =
        publish_and_create_evm_bridge(&cc_a, erc20_addr, chain_a, fungible_app_id).await?;

    // Register the wrapped-fungible app with the evm-bridge so it can drive
    // the escrow transfer + burn.
    register_bridge_app(&cc_a, fungible_app_id, bridge_app_id).await?;

    let app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
    let bridge_app_id_bytes32 = format!("0x{}", bridge_app_id.application_description_hash);
    let chain_a_bytes32 = format!("0x{chain_a}");
    let bridge_addr = deploy_fungible_bridge(
        &compose,
        project_name,
        &compose_file,
        light_client_address(),
        &chain_a_bytes32,
        erc20_addr,
        &app_id_bytes32,
        &bridge_app_id_bytes32,
    )
    .await?;

    fund_bridge_erc20(
        &compose,
        project_name,
        &compose_file,
        erc20_addr,
        bridge_addr,
        1_000_000_000_000_000_000_000u128,
    )
    .await;

    let evm_recipient: alloy::primitives::Address =
        "0x70997970C51812dc3A010C7d01b50e0d17dc79C8".parse()?;
    let burn_amount = U128(BURN_AMOUNT_TOKENS * 10u128.pow(18));

    // One Burn per chain-B block, draining chain A's inbox after each so
    // every `BurnEvent` to the same EVM recipient lands in its own
    // chain-A block. Each `BridgeOperation::Burn` routes a funding
    // transfer + tracked `BridgeMessage::Burn` to chain A; the evm-bridge
    // app escrows + burns the wrapped tokens before emitting the event.
    for _ in 0..NUM_BURNS {
        cc_b.synchronize_from_validators().await?;
        let burn_bytes = bcs::to_bytes(&BridgeOperation::Burn {
            amount: burn_amount,
            evm_target: evm_recipient.0 .0,
        })?;
        cc_b.execute_operations(
            vec![Operation::User {
                application_id: bridge_app_id,
                bytes: burn_bytes,
            }],
            vec![],
        )
        .await?
        .expect("burn committed on chain B");

        cc_a.synchronize_from_validators().await?;
        cc_a.process_inbox().await?;
    }

    let relay_dir = tempfile::tempdir()?;
    let wallet_path = relay_dir.path().join("wallet.json");
    let keystore_path = relay_dir.path().join("keystore.json");
    let storage_config = format!("rocksdb:{}", relay_dir.path().join("client.db").display());
    let sqlite_path = relay_dir.path().join("relay.sqlite3");

    {
        use linera_persistent::Persist;
        let mut ks = linera_persistent::File::new(&keystore_path, signer.clone())?;
        ks.persist().await?;
    }
    linera_wallet_json::PersistentWallet::create(&wallet_path, relay_genesis_config)?;

    let relay_port = 3004u16;
    let bridge_addr_str = format!("{bridge_addr}");
    let bridge_app_str = format!("{bridge_app_id}");
    let fungible_app_str = format!("{fungible_app_id}");
    let sqlite_path_for_relay = sqlite_path.clone();
    let relay_handle = tokio::spawn(async move {
        Box::pin(linera_bridge::relay::run(
            "http://localhost:8545",
            Some(wallet_path.as_path()),
            Some(keystore_path.as_path()),
            Some(&storage_config),
            chain_a,
            owner_a,
            &bridge_addr_str,
            &bridge_app_str,
            &fungible_app_str,
            ANVIL_PRIVATE_KEY,
            None,
            relay_port,
            0, // admin port (unused in e2e)
            linera_storage_runtime::CommonStorageOptions::with_defaults().storage_cache_config(),
            Duration::from_secs(2),
            0,
            5,
            Some(sqlite_path_for_relay.as_path()),
            None,
            2000, // max_log_block_range
        ))
        .await
    });

    let relay_url = format!("http://localhost:{relay_port}");
    let http = reqwest::Client::new();
    if let Err(error) = wait_for_relay_http_ready(&http, &relay_url, Duration::from_secs(60)).await
    {
        relay_handle.abort();
        return Err(error);
    }

    // Wait until the relayer has finished work: pending drops to 0 and
    // all NUM_BURNS appear in burns_completed. Pre-fix gets there via
    // mark-by-existence flipping every pending burn complete after the
    // first transfer lands; post-fix gets there via per-burn
    // `isBurnProcessed` flipping each entry only as its own `processBurns`
    // confirms.
    let settle_result = wait_for_relay_metrics(
        &http,
        &relay_url,
        |_detected, completed, pending, _failed| pending == 0 && completed >= i64::from(NUM_BURNS),
        Duration::from_secs(240),
    )
    .await;
    if let Err(error) = settle_result {
        relay_handle.abort();
        return Err(error);
    }

    let rpc_url = "http://localhost:8545".parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);
    let token_balance = IERC20::new(erc20_addr, &provider)
        .balanceOf(evm_recipient)
        .call()
        .await?;

    // Scrape the final metric values once for diagnostic clarity in
    // both pass and fail cases.
    let final_metrics = http
        .get(format!("{relay_url}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    let burns_detected = parse_metric_value(&final_metrics, "linera_bridge_burns_detected");

    relay_handle.abort();

    let expected_balance =
        U256::from(BURN_AMOUNT_TOKENS) * U256::from(NUM_BURNS) * U256::from(10u128.pow(18));
    tracing::info!(
        ?token_balance,
        ?expected_balance,
        burns_detected,
        "Final state"
    );

    // Sanity check: every burn must have reached the scanner. A drop
    // here would mean the test failed to pre-populate chain A
    // properly, not the bug we are reproducing.
    assert_eq!(
        burns_detected,
        i64::from(NUM_BURNS),
        "relayer must have detected all {NUM_BURNS} burns; got {burns_detected}"
    );

    // The recipient's balance must reflect every burn. Anything less
    // means at least one `PendingBurn` was marked complete without
    // its `token.transfer` actually landing on-chain — the UI-demo bug.
    assert_eq!(
        token_balance, expected_balance,
        "recipient must accumulate every burn; got {token_balance}, expected {expected_balance}"
    );

    Ok(())
}

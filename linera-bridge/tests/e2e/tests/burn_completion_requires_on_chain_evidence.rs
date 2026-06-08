// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Verifies that the relayer marks a `PendingBurn` complete only when
//! the EVM side has provably released the underlying tokens, not merely
//! because `addBlock` returned Ok.
//!
//! The setup deploys `FungibleBridge` with a `bridgeApplicationId`
//! that does not match the real evm-bridge app whose "burns" stream the
//! scanner watches, so `_onBlock`'s `_isMatchingBurn` app-id check
//! rejects every burn event the relayer forwards. `addBlock` still
//! succeeds (the cert is valid) but no `token.transfer` runs. Completion
//! is gated on the per-burn `isBurnProcessed(height, eventIndex)` query,
//! which stays false, so `linera_bridge_burns_completed` must remain at 0
//! for every detected burn.

#![recursion_limit = "512"]

use std::time::{Duration, Instant};

use alloy::{
    primitives::U256,
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};
use linera_base::{crypto::InMemorySigner, data_types::U128, identifiers::AccountOwner};
use linera_bridge::abi::BridgeOperation;
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token, fund_bridge_erc20,
    light_client_address, parse_metric_value, publish_and_create_evm_bridge,
    publish_and_create_wrapped_fungible, register_bridge_app, start_compose,
    wait_for_light_client, wait_for_relay_http_ready, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

/// `bytes32` value with no relation to any real evm-bridge application
/// description hash. Used as the `_bridgeApplicationId` in the deployed
/// `FungibleBridge` so that `_onBlock`'s `_isMatchingBurn` app-id check
/// rejects every burn event the scanner forwards (the BurnEvents are
/// emitted on the evm-bridge app's "burns" stream, matched against
/// `bridgeApplicationId`). Non-zero to satisfy the constructor guard.
const SYNTHETIC_APP_ID_BYTES32: &str =
    "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

#[tokio::test]
#[ignore] // Requires pre-built docker images, Wasm, and relay binary.
async fn relayer_does_not_mark_burn_complete_when_token_was_not_transferred() -> anyhow::Result<()>
{
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-burn-false-positive-test";

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
        "burn-false-positive-e2e",
        Some(WasmRuntime::default()),
        StorageCacheConfig {
            blob_cache_size: 1000,
            confirmed_block_cache_size: 1000,
            certificate_cache_size: 1000,
            certificate_raw_cache_size: 1000,
            event_cache_size: 1000,
            cache_cleanup_interval_secs: linera_storage::DEFAULT_CLEANUP_INTERVAL_SECS,
        },
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

    // Chain A is the bridge/mint chain: it hosts the evm-bridge app and is
    // the chain the relayer operates on. Chain B is the user chain that
    // holds the wrapped tokens and initiates the burn.
    let owner_a = AccountOwner::from(signer.generate_new());
    let chain_a_desc = faucet.claim(&owner_a).await?;
    let chain_a = chain_a_desc.id();
    ctx.extend_with_chain(chain_a_desc, Some(owner_a)).await?;
    let cc_a = ctx.make_chain_client(chain_a).await?;
    cc_a.synchronize_from_validators().await?;

    let owner_b = AccountOwner::from(signer.generate_new());
    let chain_b_desc = faucet.claim(&owner_b).await?;
    let chain_b = chain_b_desc.id();
    ctx.extend_with_chain(chain_b_desc, Some(owner_b)).await?;
    let cc_b = ctx.make_chain_client(chain_b).await?;
    cc_b.synchronize_from_validators().await?;

    let erc20_addr = deploy_linera_token(&compose, project_name, &compose_file).await?;

    // The evm-bridge app drives the burn; create it on the bridge/mint chain
    // (chain A) before the wrapped-fungible app so its id can be baked in.
    // Its `application_description_hash` is the *real* bridge app id the
    // scanner will see as the burn event's stream application. The deployed
    // `FungibleBridge` is given a *different* `bridgeApplicationId`
    // (`SYNTHETIC_APP_ID_BYTES32`) below so `_onBlock`'s `_isMatchingBurn`
    // rejects every burn event the relayer forwards.
    let fungible_app_id = publish_and_create_wrapped_fungible(
        &cc_b,
        owner_b,
        chain_a,
        erc20_addr,
        1_000 * 10u128.pow(18),
    )
    .await?;

    let bridge_app_id = publish_and_create_evm_bridge(&cc_a, erc20_addr, chain_a, fungible_app_id).await?;

    // Register the wrapped-fungible app with the evm-bridge so it can drive
    // the escrow transfer + burn.
    register_bridge_app(&cc_a, fungible_app_id, bridge_app_id).await?;

    let real_app_id_bytes32 = format!("0x{}", bridge_app_id.application_description_hash);
    assert_ne!(
        real_app_id_bytes32, SYNTHETIC_APP_ID_BYTES32,
        "real bridge app ID collided with the synthetic value used to misconfigure the bridge"
    );

    // `fungibleApplicationId` is set to the real wrapped-fungible app (used
    // only by the deposit path, which never runs here). The match that
    // `_onBlock` actually uses for burn events is `bridgeApplicationId`,
    // which we deliberately set to the non-matching synthetic value so every
    // forwarded burn event is rejected.
    let fungible_app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
    let chain_a_bytes32 = format!("0x{chain_a}");
    let bridge_addr = deploy_fungible_bridge(
        &compose,
        project_name,
        &compose_file,
        light_client_address(),
        &chain_a_bytes32,
        erc20_addr,
        &fungible_app_id_bytes32,
        SYNTHETIC_APP_ID_BYTES32,
    )
    .await?;

    // Fund the bridge so that a `token.transfer` inside `_onBlock` would
    // succeed if it ever ran. Eliminates "insufficient balance" as an
    // alternative explanation for the missing on-chain Transfer.
    fund_bridge_erc20(
        &compose,
        project_name,
        &compose_file,
        erc20_addr,
        bridge_addr,
        500 * 10u128.pow(18),
    )
    .await;

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

    // Snapshot the deployer's EVM nonce BEFORE spawning the relayer so the
    // forward-attempt wait below can distinguish a relayer-submitted
    // `addBlock` tx from the deploy/fund txs already on chain. The relayer
    // signs with the same `ANVIL_PRIVATE_KEY`, so a global nonce count
    // alone would already be `>= 1` from setup.
    let rpc_url = "http://localhost:8545".parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);
    let relayer_signer: PrivateKeySigner = ANVIL_PRIVATE_KEY.parse()?;
    let relayer_addr = relayer_signer.address();
    let nonce_before_relayer = provider.get_transaction_count(relayer_addr).await?;

    let relay_port = 3003u16;
    let bridge_addr_str = format!("{bridge_addr}");
    // The relayer monitors the evm-bridge app's "burns" stream, so its
    // bridge app ID must be the real `bridge_app_id`. It is the source of
    // the BurnEvents the scanner detects and forwards. `fungible_app_id` is
    // kept for its existing use.
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
            &linera_storage_runtime::CommonStorageOptions::with_defaults(),
            Duration::from_secs(2),
            0,
            5,
            Some(sqlite_path_for_relay.as_path()),
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

    // Trigger a single burn: chain B submits a `BridgeOperation::Burn` to
    // the evm-bridge app. The bridge app escrows + burns the wrapped tokens
    // and routes a tracked `BridgeMessage::Burn` to the bridge chain
    // (chain A); processing chain A's inbox materialises the `BurnEvent` on
    // the evm-bridge app's "burns" stream, which the relayer's Linera
    // scanner will pick up.
    let evm_recipient_hex = "70997970C51812dc3A010C7d01b50e0d17dc79C8";
    let recipient: alloy::primitives::Address = format!("0x{evm_recipient_hex}").parse()?;
    let burn_amount = U128(25u128 * 10u128.pow(18));

    cc_b.synchronize_from_validators().await?;
    let burn_bytes = bcs::to_bytes(&BridgeOperation::Burn {
        amount: burn_amount,
        evm_target: recipient.0 .0,
    })?;
    cc_b.execute_operations(
        vec![Operation::User {
            application_id: bridge_app_id,
            bytes: burn_bytes,
        }],
        vec![],
    )
    .await?
    .expect("burn operation committed on chain B");

    // Drive chain A's inbox so the tracked `BridgeMessage::Burn` is
    // processed and the `BurnEvent` lands on the evm-bridge "burns" stream.
    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;

    // Wait until the relayer has had a chance to detect the burn AND to
    // attempt forwarding it. `bridge_burns_detected` increments when the
    // scanner sees the BurnEvent; the subsequent `forward_cert` happens
    // on the next retry-loop tick. We poll for "detected ≥ 1" first, then
    // wait an extra `attempt_window` for the forwarding attempt to land.
    let mut detected = false;
    for attempt in 0..60 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if relay_handle.is_finished() {
            anyhow::bail!("Relay exited unexpectedly: {:?}", relay_handle.await);
        }
        let body = http
            .get(format!("{relay_url}/metrics"))
            .send()
            .await?
            .text()
            .await?;
        if parse_metric_value(&body, "linera_bridge_burns_detected") >= 1 {
            detected = true;
            tracing::info!(attempt, "Burn detected by scanner");
            break;
        }
    }
    if !detected {
        relay_handle.abort();
        anyhow::bail!("Relayer never detected the burn");
    }

    // Wait for the relayer to attempt forwarding by polling the relayer
    // account's EVM nonce until it goes past the pre-spawn snapshot.
    // `forward_cert` sends `addBlock` to anvil, which bumps the nonce.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut forwarded = false;
    while Instant::now() < deadline {
        if relay_handle.is_finished() {
            anyhow::bail!("Relay exited unexpectedly: {:?}", relay_handle.await);
        }
        if provider.get_transaction_count(relayer_addr).await? > nonce_before_relayer {
            forwarded = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if !forwarded {
        relay_handle.abort();
        anyhow::bail!("Relayer never submitted an EVM tx (no `addBlock` attempt observed)");
    }

    let metrics_body = http
        .get(format!("{relay_url}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    let burns_detected = parse_metric_value(&metrics_body, "linera_bridge_burns_detected");
    let burns_completed = parse_metric_value(&metrics_body, "linera_bridge_burns_completed");
    let burns_failed = parse_metric_value(&metrics_body, "linera_bridge_burns_failed");
    let burns_pending = parse_metric_value(&metrics_body, "linera_bridge_burns_pending");

    let token_balance = IERC20::new(erc20_addr, &provider)
        .balanceOf(recipient)
        .call()
        .await?;

    relay_handle.abort();

    tracing::info!(
        burns_detected,
        burns_completed,
        burns_failed,
        burns_pending,
        ?token_balance,
        "Final state after relayer cycle"
    );

    // Sanity: the misconfiguration scenario is reproduced. The contract's
    // `_onBlock` rejected every burn event (app ID mismatch), so no
    // `token.transfer` ran.
    assert_eq!(
        token_balance,
        U256::ZERO,
        "no on-chain Transfer should have happened — \
         _onBlock skips every event because bridgeApplicationId is mismatched"
    );

    // Bug demonstration: the relayer must not mark a burn as completed
    // unless the EVM contract actually released the tokens.
    assert_eq!(
        burns_completed, 0,
        "relayer marked a burn complete despite no on-chain Transfer; \
         token.balanceOf(recipient) = 0 but linera_bridge_burns_completed = {burns_completed}"
    );

    Ok(())
}

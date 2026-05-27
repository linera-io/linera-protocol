// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Verifies that multiple `BurnEvent`s in a single Linera block are all
//! relayed and completed on the EVM side. The user chain submits one
//! block carrying N `Transfer` operations to N distinct `Address20`
//! recipients on the bridge chain. The test process drives the inbox
//! processing on the bridge chain so the resulting `Credit` messages
//! all land in one chain-A block — a single cert with N `BurnEvent`s.
//! After the relayer is spawned, exactly one `addBlock` call should
//! release all N tokens (one `token.transfer` per burn in `_onBlock`)
//! and the relayer must mark every pending burn complete via the
//! per-burn `isBurnProcessed(height, eventIndex)` view.

#![recursion_limit = "512"]

use std::time::Duration;

use alloy::{primitives::U256, providers::ProviderBuilder, sol};
use linera_base::{crypto::InMemorySigner, data_types::U128, identifiers::AccountOwner};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token, fund_bridge_erc20,
    light_client_address, parse_metric_value, publish_and_create_wrapped_fungible, start_compose,
    wait_for_light_client, wait_for_relay_http_ready, wait_for_relay_metrics, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use wrapped_fungible::{Account, WrappedFungibleOperation};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

const NUM_BURNS: usize = 4;
const BURN_AMOUNT_TOKENS: u128 = 1;

#[tokio::test]
#[ignore] // Requires pre-built docker images, Wasm, and relay binary.
async fn relayer_processes_every_burn_in_one_block() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-multi-burn-same-block-test";

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
        "multi-burn-same-block-e2e",
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

    // Chain A hosts the bridge contract.
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

    let fungible_app_id =
        publish_and_create_wrapped_fungible(&cc_b, owner_b, chain_a, erc20_addr, 1_000).await?;

    let app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
    let chain_a_bytes32 = format!("0x{chain_a}");
    let bridge_addr = deploy_fungible_bridge(
        &compose,
        project_name,
        &compose_file,
        light_client_address(),
        &chain_a_bytes32,
        erc20_addr,
        &app_id_bytes32,
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

    // NUM_BURNS Address20 recipients in the order they appear in the
    // block. One address is intentionally repeated to exercise per-burn
    // accounting independently of the recipient; `_onBlock` must release
    // tokens for each occurrence even though the dedup key shares no
    // recipient bits.
    let recipients: [alloy::primitives::Address; NUM_BURNS] = [
        "0x70997970C51812dc3A010C7d01b50e0d17dc79C8".parse()?,
        "0x70997970C51812dc3A010C7d01b50e0d17dc79C8".parse()?,
        "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC".parse()?,
        "0x90F79bf6EB2c4f870365E785982E1f101E93b906".parse()?,
    ];
    let burn_amount = U128(BURN_AMOUNT_TOKENS * 10u128.pow(18));

    // Bundle every Transfer into a SINGLE chain-B block, then drive
    // chain A's inbox once. The N Credit messages travel together
    // and `process_inbox` materialises one chain-A block containing
    // all N BurnEvents.
    let operations = recipients
        .iter()
        .map(|recipient| {
            let owner = AccountOwner::Address20(recipient.0 .0);
            let withdraw_bytes = bcs::to_bytes(&WrappedFungibleOperation::Transfer {
                owner: owner_b,
                amount: burn_amount,
                target_account: Account {
                    chain_id: chain_a,
                    owner,
                },
            })
            .expect("BCS serialization");
            Operation::User {
                application_id: fungible_app_id,
                bytes: withdraw_bytes,
            }
        })
        .collect();

    cc_b.synchronize_from_validators().await?;
    cc_b.execute_operations(operations, vec![])
        .await?
        .expect("multi-burn block committed on chain B");

    cc_a.synchronize_from_validators().await?;
    let height_before_inbox = cc_a.chain_info().await?.next_block_height;
    cc_a.process_inbox().await?;
    let height_after_inbox = cc_a.chain_info().await?.next_block_height;
    assert_eq!(
        height_after_inbox.0,
        height_before_inbox.0 + 1,
        "chain A must produce exactly ONE block carrying all {NUM_BURNS} BurnEvents \
         (before={height_before_inbox}, after={height_after_inbox})",
    );

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

    let relay_port = 3005u16;
    let bridge_addr_str = format!("{bridge_addr}");
    let bridge_app_str = format!("{fungible_app_id}");
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
            linera_storage_runtime::CommonStorageOptions::with_defaults().storage_cache_config(),
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

    let num_burns_i64 = i64::try_from(NUM_BURNS).unwrap();
    // Gate on the scanner having found every burn; we deliberately do
    // NOT wait for `pending == 0` because the failure mode we want to
    // surface is a balance / counter discrepancy, not a timeout —
    // assertions below catch the actual state.
    if let Err(error) = wait_for_relay_metrics(
        &http,
        &relay_url,
        |detected, _completed, _pending, _failed| detected >= num_burns_i64,
        Duration::from_secs(60),
    )
    .await
    {
        relay_handle.abort();
        return Err(error);
    }
    // Wait for the relayer to finish: one `addBlock` releases all N
    // tokens inside `_onBlock`, then `check_burn_completion`'s per-burn
    // `isBurnProcessed` query flips every entry to completed.
    if let Err(error) = wait_for_relay_metrics(
        &http,
        &relay_url,
        |_detected, completed, _pending, _failed| completed >= num_burns_i64,
        Duration::from_secs(60),
    )
    .await
    {
        relay_handle.abort();
        return Err(error);
    }

    let rpc_url = "http://localhost:8545".parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);
    let token = IERC20::new(erc20_addr, &provider);

    // Each occurrence in `recipients` is one expected transfer. A
    // recipient listed twice should accumulate two transfers, etc.
    let mut expected_per_recipient =
        std::collections::BTreeMap::<alloy::primitives::Address, U256>::new();
    let one_burn = U256::from(BURN_AMOUNT_TOKENS) * U256::from(10u128.pow(18));
    for recipient in &recipients {
        *expected_per_recipient
            .entry(*recipient)
            .or_insert(U256::ZERO) += one_burn;
    }

    let mut observed_balances = Vec::with_capacity(expected_per_recipient.len());
    for (recipient, expected) in &expected_per_recipient {
        let balance = token.balanceOf(*recipient).call().await?;
        observed_balances.push((*recipient, balance, *expected));
    }

    let final_metrics = http
        .get(format!("{relay_url}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    let burns_detected = parse_metric_value(&final_metrics, "linera_bridge_burns_detected");
    let burns_completed = parse_metric_value(&final_metrics, "linera_bridge_burns_completed");

    relay_handle.abort();

    tracing::info!(
        ?observed_balances,
        burns_detected,
        burns_completed,
        "Final state"
    );

    // Sanity: the scanner must have picked up all NUM_BURNS as separate
    // PendingBurns even though they share a Linera block.
    assert_eq!(
        burns_detected, num_burns_i64,
        "relayer must have detected all {NUM_BURNS} burns; got {burns_detected}"
    );
    assert_eq!(
        burns_completed, num_burns_i64,
        "relayer must have completed all {NUM_BURNS} burns; got {burns_completed}"
    );

    // Each unique recipient must hold exactly the sum of their
    // occurrences in `recipients`. Anything less means a per-burn
    // release was dropped; anything more means double-spend.
    for (recipient, balance, expected) in &observed_balances {
        assert_eq!(
            *balance, *expected,
            "recipient {recipient:?} balance {balance}, expected {expected}"
        );
    }

    Ok(())
}

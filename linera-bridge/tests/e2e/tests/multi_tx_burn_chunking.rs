// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Verifies that when a Linera block's `addBlock(cert)` would exceed the
//! EVM block gas limit, the relayer falls back to chunked
//! `processBurns(cert, txIndex, positionsInTx)` calls and every burn
//! still settles correctly.
//!
//! Setup: anvil's block gas limit is dialled down (via
//! `evm_setBlockGasLimit`) before the bridge is deployed, so `addBlock`
//! at `NUM_BURNS = 8` does not fit and the relayer must take the
//! `processBurns` chunk path. Chain B submits one block with N
//! `Transfer` operations to N distinct recipients on chain A. After the
//! relayer is spawned, every recipient must hold the correct ERC-20
//! balance and `linera_bridge_burns_completed` must reach N.

#![recursion_limit = "512"]

use std::time::Duration;

use std::collections::HashSet;

use alloy::{
    primitives::{B256, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::Filter,
    sol,
};
use linera_base::{crypto::InMemorySigner, data_types::U128, identifiers::AccountOwner};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token, fund_bridge_erc20,
    light_client_address, parse_metric_value, publish_and_create_wrapped_fungible,
    set_anvil_block_gas_limit, start_compose, wait_for_light_client, wait_for_relay_http_ready,
    wait_for_relay_metrics, ANVIL_PRIVATE_KEY,
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

const NUM_BURNS: usize = 8;
const BURN_AMOUNT_TOKENS: u128 = 1;
/// Per-block gas ceiling sized to live between `processBurns(cert, tx, [single])`
/// (which is dominated by cert verification, ~2–2.5M gas) and `addBlock(cert)`
/// for `NUM_BURNS` burns (~3.3M gas observed in `burns_per_evm_tx`). The
/// relayer must therefore route through chunked `processBurns` per tx.
const ANVIL_BLOCK_GAS_LIMIT: u64 = 3_000_000;

#[tokio::test]
#[ignore] // Requires pre-built docker images, Wasm, and relay binary.
async fn relayer_falls_back_to_chunked_process_burns() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-multi-tx-burn-chunking-test";

    let compose = start_compose(&compose_file, project_name).await;
    wait_for_light_client(&compose, project_name, &compose_file).await;

    // Provider is held for the post-deploy gas-limit drop and for the
    // final balance reads. Deploy txs need anvil's default (high) limit;
    // we tighten it once the bridge is live.
    let rpc_url = "http://localhost:8545".parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let faucet = Faucet::new("http://localhost:8080".to_string());
    let genesis_config = faucet.genesis_config().await?;
    let relay_genesis_config = genesis_config.clone();

    let store_config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &store_config,
        "multi-tx-burn-chunking-e2e",
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

    // Now tighten the block gas limit so `addBlock(cert)` for `NUM_BURNS`
    // doesn't fit and the relayer is forced through the chunked
    // `processBurns` path. Deploy + funding txs already landed.
    set_anvil_block_gas_limit(&provider, ANVIL_BLOCK_GAS_LIMIT).await?;

    // Distinct recipients so each burn produces a distinct ERC-20 balance.
    let recipients: [alloy::primitives::Address; NUM_BURNS] = [
        "0x70997970C51812dc3A010C7d01b50e0d17dc79C8".parse()?,
        "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC".parse()?,
        "0x90F79bf6EB2c4f870365E785982E1f101E93b906".parse()?,
        "0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65".parse()?,
        "0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc".parse()?,
        "0x976EA74026E726554dB657fA54763abd0C3a0aa9".parse()?,
        "0x14dC79964da2C08b23698B3D3cc7Ca32193d9955".parse()?,
        "0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f".parse()?,
    ];
    let burn_amount = U128(BURN_AMOUNT_TOKENS * 10u128.pow(18));

    // Bundle every Transfer into one chain-B block; chain-A's
    // process_inbox produces a single chain-A block with N BurnEvents.
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

    let relay_port = 3009u16;
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

    let num_burns_i64 = i64::try_from(NUM_BURNS).unwrap();
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
    // Wait for the chunked-processBurns path to settle every burn.
    // Allow more wall time than the single-addBlock test because each
    // chunk is its own EVM tx + receipt round-trip.
    if let Err(error) = wait_for_relay_metrics(
        &http,
        &relay_url,
        |_detected, completed, _pending, _failed| completed >= num_burns_i64,
        Duration::from_secs(180),
    )
    .await
    {
        relay_handle.abort();
        return Err(error);
    }

    let token = IERC20::new(erc20_addr, &provider);
    let one_burn = U256::from(BURN_AMOUNT_TOKENS) * U256::from(10u128.pow(18));
    let mut observed_balances = Vec::with_capacity(recipients.len());
    for recipient in &recipients {
        let balance = token.balanceOf(*recipient).call().await?;
        observed_balances.push((*recipient, balance));
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

    assert_eq!(
        burns_detected, num_burns_i64,
        "relayer must have detected all {NUM_BURNS} burns; got {burns_detected}"
    );
    assert_eq!(
        burns_completed, num_burns_i64,
        "relayer must have completed all {NUM_BURNS} burns; got {burns_completed}"
    );

    for (recipient, balance) in &observed_balances {
        assert_eq!(
            *balance, one_burn,
            "recipient {recipient:?} balance {balance}, expected {one_burn}"
        );
    }

    // The whole point of this test is the chunked-processBurns path, which
    // splits the cert across multiple EVM transactions. Each chunk lands in
    // its own EVM block. Verify by counting distinct block hashes among
    // ERC-20 `Transfer` events emitted by the bridge contract: if `addBlock`
    // had fit, all 8 transfers would share one EVM block.
    //
    // `Transfer(address,address,uint256)` topic0.
    let transfer_sig = B256::from(alloy::primitives::keccak256(
        "Transfer(address,address,uint256)",
    ));
    let bridge_topic = B256::left_padding_from(bridge_addr.as_slice());
    let transfer_logs = provider
        .get_logs(
            &Filter::new()
                .address(erc20_addr)
                .event_signature(transfer_sig)
                .topic1(bridge_topic)
                .from_block(0u64),
        )
        .await?;
    let transfer_blocks: HashSet<B256> =
        transfer_logs.iter().filter_map(|l| l.block_hash).collect();
    assert!(
        transfer_blocks.len() > 1,
        "expected chunked processBurns path to span multiple EVM blocks; \
         saw {} distinct block(s) across {} bridge-originated Transfer logs",
        transfer_blocks.len(),
        transfer_logs.len(),
    );

    Ok(())
}

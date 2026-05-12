// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Verifies that the relayer marks a `PendingBurn` complete only when
//! the EVM side has provably released the underlying tokens, not merely
//! because `addBlock` returned Ok.
//!
//! The setup deploys `FungibleBridge` with a `fungibleApplicationId`
//! that does not match the real wrapped-fungible app the scanner
//! watches, so `_onBlock`'s app-id check rejects every burn event the
//! relayer forwards. `addBlock` still succeeds (the cert is valid)
//! but no `token.transfer` runs. Completion is gated on the per-burn
//! `isBurnProcessed(height, eventIndex)` query, which stays false, so
//! `linera_bridge_burns_completed` must remain at 0 for every detected
//! burn.

#![recursion_limit = "512"]

use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use alloy::{
    primitives::U256,
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};
use anyhow::Context as _;
use linera_base::{
    crypto::InMemorySigner,
    data_types::{Amount, Bytecode},
    identifiers::AccountOwner,
    vm::VmRuntime,
};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token, exec_ok, light_client_address,
    start_compose, wait_for_light_client, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use wrapped_fungible::{Account, InitialState, WrappedFungibleOperation, WrappedParameters};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

/// `bytes32` value with no relation to any real wrapped-fungible application
/// description hash. Used as the `_fungibleApplicationId` in the deployed
/// `FungibleBridge` so that `_onBlock`'s app-id check rejects every burn
/// event the scanner forwards. Non-zero to satisfy the constructor guard.
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

    // Chain A hosts the wrapped-fungible app and is the chain the relayer
    // operates on. Chain B is the user chain initiating the burn.
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

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .context("manifest dir has fewer than 3 ancestors")?
        .to_path_buf();
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");

    // Publish + create the wrapped-fungible application on chain B (the
    // user chain) with `mint_chain_id = chain_a` (the bridge / relayer
    // chain). Initial balance goes to `owner_b`, the user. The created
    // app's `application_description_hash` is the *real* fungible_app_id
    // the scanner will see in burn events. The deployed `FungibleBridge`
    // is given a *different* id (`SYNTHETIC_APP_ID_BYTES32`) below so
    // `_onBlock` rejects every burn event the relayer forwards.
    let wf_contract = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_contract.wasm"))?;
    let wf_service = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_service.wasm"))?;
    let (wf_module_id, _) = cc_b
        .publish_module(wf_contract, wf_service, VmRuntime::Wasm)
        .await?
        .expect("publish wrapped-fungible module committed");
    cc_b.synchronize_from_validators().await?;
    cc_b.process_inbox().await?;

    let initial_balance = Amount::from_tokens(1_000);
    let (fungible_app_id, _) = cc_b
        .create_application_untyped(
            wf_module_id,
            serde_json::to_vec(&WrappedParameters {
                ticker_symbol: "wTEST".to_string(),
                minter: None,
                mint_chain_id: Some(chain_a),
                evm_token_address: erc20_addr.0 .0,
                evm_source_chain_id: 31337,
                bridge_app_id: None,
            })?,
            serde_json::to_vec(&InitialState {
                accounts: BTreeMap::from([(owner_b, initial_balance)]),
            })?,
            vec![],
        )
        .await?
        .expect("create wrapped-fungible app committed");
    let real_app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
    assert_ne!(
        real_app_id_bytes32, SYNTHETIC_APP_ID_BYTES32,
        "real app ID collided with the synthetic value used to misconfigure the bridge"
    );

    let chain_a_bytes32 = format!("0x{chain_a}");
    let bridge_addr = deploy_fungible_bridge(
        &compose,
        project_name,
        &compose_file,
        light_client_address(),
        &chain_a_bytes32,
        erc20_addr,
        SYNTHETIC_APP_ID_BYTES32,
    )
    .await?;

    // Fund the bridge so that a `token.transfer` inside `_onBlock` would
    // succeed if it ever ran. Eliminates "insufficient balance" as an
    // alternative explanation for the missing on-chain Transfer.
    exec_ok(
        &compose,
        "foundry-tools",
        &format!(
            "cast send --rpc-url http://anvil:8545 \
             --private-key {ANVIL_PRIVATE_KEY} \
             {erc20_addr} \
             'transfer(address,uint256)(bool)' \
             {bridge_addr} \
             500000000000000000000"
        ),
        project_name,
        &compose_file,
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
    // The relayer needs an evm-bridge app ID even though this test never
    // uses the EVM→Linera direction. Reuse the wrapped-fungible app ID as
    // a syntactically valid placeholder — no deposit flow runs against it.
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
    for attempt in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if http
            .get(format!("{relay_url}/metrics"))
            .send()
            .await
            .is_ok()
        {
            break;
        }
        if attempt == 29 {
            relay_handle.abort();
            anyhow::bail!("Relay did not become ready");
        }
    }

    // Trigger a single burn: chain B transfers wrapped tokens to an
    // Address20 owner on chain A. The wrapped-fungible app on chain A
    // detects the Credit-to-Address20 message and emits a `BurnEvent` on
    // the "burns" stream, which the relayer's Linera scanner will pick up.
    let evm_recipient_hex = "70997970C51812dc3A010C7d01b50e0d17dc79C8";
    let receiver: AccountOwner = format!("0x{evm_recipient_hex}").parse()?;
    let burn_amount = Amount::from_tokens(25);

    cc_b.synchronize_from_validators().await?;
    let withdraw_bytes = bcs::to_bytes(&WrappedFungibleOperation::Transfer {
        owner: owner_b,
        amount: burn_amount,
        target_account: Account {
            chain_id: chain_a,
            owner: receiver,
        },
    })?;
    cc_b.execute_operations(
        vec![Operation::User {
            application_id: fungible_app_id,
            bytes: withdraw_bytes,
        }],
        vec![],
    )
    .await?
    .expect("cross-chain withdrawal committed on chain B");

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
        if metric_value(&body, "linera_bridge_burns_detected") >= 1 {
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
    let burns_detected = metric_value(&metrics_body, "linera_bridge_burns_detected");
    let burns_completed = metric_value(&metrics_body, "linera_bridge_burns_completed");
    let burns_failed = metric_value(&metrics_body, "linera_bridge_burns_failed");
    let burns_pending = metric_value(&metrics_body, "linera_bridge_burns_pending");

    let evm_recipient: alloy::primitives::Address = format!("0x{evm_recipient_hex}").parse()?;
    let token_balance = IERC20::new(erc20_addr, &provider)
        .balanceOf(evm_recipient)
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
         _onBlock skips every event because fungibleApplicationId is mismatched"
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

/// Parses a Prometheus text-format response and returns the integer value
/// associated with a metric name. Returns 0 if the metric is absent
/// (counters are reported only after their first increment).
fn metric_value(body: &str, name: &str) -> i64 {
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some((metric, value)) = line.split_once(' ') else {
            continue;
        };
        if metric.trim() == name {
            return value.trim().parse::<f64>().unwrap_or(0.0) as i64;
        }
    }
    0
}

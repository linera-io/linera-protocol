// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for both bridge directions with automatic scanning:
//! 1. EVM→Linera: deposit on EVM without calling `/deposit`, relay scanner auto-processes.
//! 2. Linera→EVM: transfer tokens to Address20 cross-chain, relay detects burn and forwards.
//!
//! Chain layout:
//! - Chain A (relay): evm-bridge + wrapped-fungible apps, operated exclusively by the relay
//! - Chain B (user): test operates here, never touches chain A directly
//!
//! Deploy order (same as setup.sh):
//! 1. MockERC20
//! 2. wrapped-fungible app (Linera)
//! 3. FungibleBridge with real applicationId (EVM)
//! 4. evm-bridge app with bridge address (Linera)

#![recursion_limit = "512"]

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use alloy::{
    network::EthereumWallet,
    primitives::{FixedBytes, U256},
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
    sol,
};
use anyhow::Context as _;
use linera_base::{
    crypto::InMemorySigner,
    data_types::{Amount, Bytecode},
    identifiers::{AccountOwner, ApplicationId},
    vm::VmRuntime,
};
use linera_bridge_e2e::{
    compose_file_path, exec_ok, exec_output, light_client_address, parse_deployed_address,
    start_compose, wait_for_light_client, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use serde::Serialize;
use wrapped_fungible::{Account, InitialState, WrappedFungibleOperation, WrappedParameters};

// ── Inline evm-bridge types ──

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct BridgeParameters {
    source_chain_id: u64,
    bridge_contract_address: [u8; 20],
    token_address: [u8; 20],
    rpc_endpoint: String,
}

#[derive(Debug, Serialize)]
enum BridgeOperation {
    RegisterFungibleApp { app_id: ApplicationId },
}

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function balanceOf(address account) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IFungibleBridge {
        function deposit(
            bytes32 target_chain_id,
            bytes32 target_application_id,
            bytes32 target_account_owner,
            uint256 amount
        ) external;
    }
}

#[tokio::test]
#[ignore] // Requires pre-built docker images, Wasm, and relay binary
async fn test_auto_deposit_scan() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-auto-scan-test";

    // ── Phase 1: Start docker compose stack ──
    let compose = start_compose(&compose_file, project_name).await;
    wait_for_light_client(&compose, project_name, &compose_file).await;

    // ── Phase 2: Create Linera client, claim chains ──
    tracing::info!("Creating programmatic Linera client...");
    let faucet = Faucet::new("http://localhost:8080".to_string());
    let genesis_config = faucet.genesis_config().await?;
    // Snapshot for later — relay::run now requires a pre-existing wallet on disk.
    let relay_genesis_config = genesis_config.clone();

    let config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &config,
        "auto-scan-e2e-test",
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

    // Chain A: relay chain.
    tracing::info!("Claiming chain A (relay)...");
    let owner_a = AccountOwner::from(signer.generate_new());
    let chain_a_desc = faucet.claim(&owner_a).await?;
    let chain_a = chain_a_desc.id();
    ctx.extend_with_chain(chain_a_desc, Some(owner_a)).await?;
    let cc_a = ctx.make_chain_client(chain_a).await?;
    cc_a.synchronize_from_validators().await?;
    tracing::info!(%chain_a, "Chain A claimed");

    // Chain B: user chain.
    tracing::info!("Claiming chain B (user)...");
    let owner_b = AccountOwner::from(signer.generate_new());
    let chain_b_desc = faucet.claim(&owner_b).await?;
    let chain_b = chain_b_desc.id();
    ctx.extend_with_chain(chain_b_desc, Some(owner_b)).await?;
    let cc_b = ctx.make_chain_client(chain_b).await?;
    cc_b.synchronize_from_validators().await?;
    tracing::info!(%chain_b, "Chain B claimed");

    // ── Phase 3: Deploy MockERC20 ──
    tracing::info!("Deploying MockERC20...");
    let erc20_output = exec_output(
        &compose,
        "foundry-tools",
        &format!(
            "forge create /contracts/MockERC20.sol:MockERC20 \
             --root /contracts --via-ir --optimize \
             --evm-version shanghai \
             --out /tmp/forge-out --cache-path /tmp/forge-cache \
             --rpc-url http://anvil:8545 \
             --broadcast \
             --private-key {ANVIL_PRIVATE_KEY} \
             --constructor-args \"TestToken\" \"TT\" 1000000000000000000000"
        ),
        project_name,
        &compose_file,
    )
    .await;
    let erc20_addr = parse_deployed_address(&erc20_output)?;
    tracing::info!(%erc20_addr, "MockERC20 deployed");

    // ── Phase 4: Deploy FungibleBridge on EVM (no applicationId needed at construction) ──
    let chain_a_bytes32 = format!("0x{chain_a}");
    let light_client = light_client_address();

    tracing::info!("Deploying FungibleBridge...");
    let bridge_output = exec_output(
        &compose,
        "foundry-tools",
        &format!(
            "forge create /contracts/FungibleBridge.sol:FungibleBridge \
             --root /contracts --via-ir --optimize \
             --ignored-error-codes 6321 \
             --evm-version shanghai \
             --out /tmp/forge-out --cache-path /tmp/forge-cache \
             --rpc-url http://anvil:8545 \
             --private-key {ANVIL_PRIVATE_KEY} \
             --broadcast \
             --constructor-args \
             {light_client} \
             {chain_a_bytes32} \
             {erc20_addr}"
        ),
        project_name,
        &compose_file,
    )
    .await;
    let bridge_addr = parse_deployed_address(&bridge_output)?;
    tracing::info!(%bridge_addr, "FungibleBridge deployed");

    tracing::info!("Funding FungibleBridge with ERC20 tokens...");
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

    // ── Phase 5: Publish and deploy Linera apps ──
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .context("manifest dir has fewer than 3 ancestors")?
        .to_path_buf();
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");

    tracing::info!("Publishing evm-bridge module...");
    let eb_contract = Bytecode::load_from_file(wasm_dir.join("evm_bridge_contract.wasm"))?;
    let eb_service = Bytecode::load_from_file(wasm_dir.join("evm_bridge_service.wasm"))?;
    let (eb_module_id, _) = cc_a
        .publish_module(eb_contract, eb_service, VmRuntime::Wasm)
        .await?
        .expect("publish evm-bridge module committed");
    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;

    tracing::info!("Creating evm-bridge application...");
    let (bridge_app_id, _) = cc_a
        .create_application_untyped(
            eb_module_id,
            serde_json::to_vec(&BridgeParameters {
                source_chain_id: 31337,
                bridge_contract_address: bridge_addr.0 .0,
                token_address: erc20_addr.0 .0,
                rpc_endpoint: String::new(),
            })?,
            serde_json::to_vec(&())?,
            vec![],
        )
        .await?
        .expect("create evm-bridge app committed");
    tracing::info!(%bridge_app_id, "evm-bridge app created");

    tracing::info!("Publishing wrapped-fungible module...");
    let wf_contract = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_contract.wasm"))?;
    let wf_service = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_service.wasm"))?;
    let (wf_module_id, _) = cc_a
        .publish_module(wf_contract, wf_service, VmRuntime::Wasm)
        .await?
        .expect("publish wrapped-fungible module committed");
    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;

    tracing::info!("Creating wrapped-fungible application...");
    let (fungible_app_id, _) = cc_a
        .create_application_untyped(
            wf_module_id,
            serde_json::to_vec(&WrappedParameters {
                ticker_symbol: "wTEST".to_string(),
                minter: Some(owner_a),
                mint_chain_id: Some(chain_a),
                evm_token_address: erc20_addr.0 .0,
                evm_source_chain_id: 31337,
                bridge_app_id: Some(bridge_app_id),
            })?,
            serde_json::to_vec(&InitialState {
                accounts: BTreeMap::new(),
            })?,
            vec![],
        )
        .await?
        .expect("create wrapped-fungible app committed");
    tracing::info!(%fungible_app_id, "wrapped-fungible app created");

    // ── Phase 6: Register app IDs on both sides ──
    tracing::info!("Registering fungible app in evm-bridge...");
    let register_bytes = bcs::to_bytes(&BridgeOperation::RegisterFungibleApp {
        app_id: fungible_app_id,
    })?;
    let register_operation = Operation::User {
        application_id: bridge_app_id,
        bytes: register_bytes,
    };
    cc_a.execute_operations(vec![register_operation], vec![])
        .await?
        .expect("register fungible app committed");

    let app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
    tracing::info!("Registering fungibleApplicationId in FungibleBridge...");
    exec_ok(
        &compose,
        "foundry-tools",
        &format!(
            "cast send --rpc-url http://anvil:8545 \
             --private-key {ANVIL_PRIVATE_KEY} \
             {bridge_addr} \
             'registerFungibleApplicationId(bytes32)' \
             {app_id_bytes32}"
        ),
        project_name,
        &compose_file,
    )
    .await;
    tracing::info!("All app IDs registered");

    // ── Phase 7: Start relay ──
    let rpc_url = "http://localhost:8545".parse()?;
    let evm_signer: PrivateKeySigner = ANVIL_PRIVATE_KEY.parse()?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .connect_http(rpc_url);

    let relay_dir = tempfile::tempdir()?;
    let wallet_path = relay_dir.path().join("wallet.json");
    let keystore_path = relay_dir.path().join("keystore.json");
    let storage_config = format!("rocksdb:{}", relay_dir.path().join("client.db").display());

    {
        use linera_persistent::Persist;
        let mut ks = linera_persistent::File::new(&keystore_path, signer.clone())?;
        ks.persist().await?;
    }

    // Pre-bootstrap the relay's wallet — `linera_bridge::relay::run` no longer
    // auto-creates one from a faucet; it expects an existing wallet on disk.
    linera_wallet_json::PersistentWallet::create(&wallet_path, relay_genesis_config)?;

    let relay_port = 3002u16;
    let bridge_addr_str = format!("{bridge_addr}");
    let bridge_app_str = format!("{bridge_app_id}");
    let fungible_app_str = format!("{fungible_app_id}");
    tracing::info!("Starting relay...");
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
            5,  // monitor_scan_interval
            0,  // monitor_start_block
            5,  // max_retries
            None,
        ))
        .await
    });

    let relay_url = format!("http://localhost:{relay_port}");
    let client = reqwest::Client::new();
    for attempt in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if client
            .get(format!("{relay_url}/metrics"))
            .send()
            .await
            .is_ok()
        {
            tracing::info!(attempt, "Relay is ready");
            break;
        }
        if attempt == 29 {
            relay_handle.abort();
            anyhow::bail!("Relay did not become ready");
        }
    }

    // Diagnostic: check chain A height from test's perspective.
    cc_a.synchronize_from_validators().await?;
    let info = cc_a.chain_info().await?;
    tracing::info!(next_block_height = ?info.next_block_height, "Chain A height from test client");

    // ══════════════════════════════════════════════════════════════════
    // Phase 8: EVM→Linera deposit targeting chain B
    // ══════════════════════════════════════════════════════════════════
    let deposit_amount = U256::from(50u128 * 10u128.pow(18));

    tracing::info!("Approving FungibleBridge...");
    let erc20_contract = IERC20::new(erc20_addr, &provider);
    erc20_contract
        .approve(bridge_addr, deposit_amount)
        .send()
        .await?
        .get_receipt()
        .await?;

    let chain_b_b256 = {
        let bytes: [u8; 32] = chain_b.0.into();
        FixedBytes::<32>::from(bytes)
    };
    let owner_b_b256 = match owner_b {
        AccountOwner::Address32(hash) => {
            let bytes: [u8; 32] = hash.into();
            FixedBytes::<32>::from(bytes)
        }
        _ => anyhow::bail!("expected Address32 owner"),
    };

    tracing::info!("Depositing on EVM targeting chain B...");
    let bridge_contract = IFungibleBridge::new(bridge_addr, &provider);
    let deposit_receipt = bridge_contract
        .deposit(
            chain_b_b256,
            app_id_bytes32.parse()?,
            owner_b_b256,
            deposit_amount,
        )
        .send()
        .await?
        .get_receipt()
        .await?;
    tracing::info!("Deposit confirmed on EVM");

    let deposit_key = linera_bridge::proof::DepositKey {
        source_chain_id: 31337,
        block_hash: deposit_receipt.block_hash.unwrap().0,
        tx_index: 0,
        log_index: 0,
    };

    // Wait for relay to auto-process the deposit.
    tracing::info!("Waiting for relay scanner to auto-process the deposit...");
    for attempt in 0..60 {
        tokio::time::sleep(Duration::from_secs(5)).await;

        if relay_handle.is_finished() {
            anyhow::bail!("Relay exited unexpectedly: {:?}", relay_handle.await);
        }

        // Sync chain B to receive minted tokens.
        cc_b.synchronize_from_validators().await?;
        cc_b.process_inbox().await?;

        // Check on-chain whether the deposit was processed.
        match linera_bridge_e2e::query_deposit_processed(&cc_a, bridge_app_id, &deposit_key).await {
            Ok(true) => {
                tracing::info!(attempt, "Deposit auto-processed!");
                break;
            }
            Ok(false) => {
                tracing::info!(attempt, "Deposit not yet processed, waiting...");
            }
            Err(e) => {
                tracing::warn!(attempt, "Deposit query failed: {e:#}");
            }
        }
        if attempt == 59 {
            relay_handle.abort();
            anyhow::bail!("Deposit not auto-processed within timeout");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 9: Linera→EVM burn via cross-chain transfer
    // ══════════════════════════════════════════════════════════════════
    let evm_recipient = "70997970C51812dc3A010C7d01b50e0d17dc79C8";
    let receiver: AccountOwner = format!("0x{evm_recipient}").parse()?;
    let withdraw_amount = Amount::from_tokens(25);

    tracing::info!("Sending cross-chain withdrawal from chain B to Address20 on chain A...");
    cc_b.synchronize_from_validators().await?;
    let withdraw_bytes = bcs::to_bytes(&WrappedFungibleOperation::Transfer {
        owner: owner_b,
        amount: withdraw_amount,
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
    .expect("withdrawal committed");
    tracing::info!("Cross-chain withdrawal committed on chain B");

    // Wait for relay to burn and forward to EVM.
    tracing::info!("Waiting for ERC-20 balance...");
    let evm_recipient_addr: alloy::primitives::Address = format!("0x{evm_recipient}").parse()?;
    let expected_balance = U256::from(25u128 * 10u128.pow(18));

    for attempt in 0..60 {
        tokio::time::sleep(Duration::from_secs(5)).await;

        if relay_handle.is_finished() {
            anyhow::bail!("Relay exited unexpectedly: {:?}", relay_handle.await);
        }

        let balance = erc20_contract.balanceOf(evm_recipient_addr).call().await?;
        tracing::info!(attempt, ?balance, "ERC-20 balance");

        if balance >= expected_balance {
            relay_handle.abort();
            tracing::info!("Test passed! Both directions: EVM→Linera deposit + Linera→EVM burn.");
            return Ok(());
        }
    }

    relay_handle.abort();
    anyhow::bail!("Burn not forwarded to EVM within timeout");
}

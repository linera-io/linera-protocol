// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

//! End-to-end test demonstrating the Linera→EVM leg of the decimal-mismatch
//! bug against a non-18-decimal ERC-20 (here: 6, like USDC).
//!
//! `Amount::from_tokens(1)` is `Amount(10^18)` regardless of the source
//! ERC-20's actual decimals. When the auto-burn emits a `BurnEvent`
//! carrying that amount, the EVM `FungibleBridge` calls
//! `token.transfer(target, 10^18)` — for a 6-decimal token that releases
//! `10^12` whole tokens, not 1.
//!
//! Flow:
//! 1. Seed `owner_a` on chain A with `Amount::from_tokens(1)` via
//!    `InitialState`.
//! 2. `owner_a` cross-chain Transfer 1 wTEST → `(chain_b, owner_b)`.
//! 3. `owner_b` cross-chain Transfer 1 wTEST → `(chain_a, Address20)`.
//!    Triggers auto-burn on chain A and emits a `BurnEvent`.
//! 4. Submit chain A certs to `FungibleBridge.addBlock`.
//! 5. Assert EVM recipient balance equals `ONE_EVM_TOKEN_RAW`
//!    (= 1 whole token on the 6-decimal ERC-20).
//!
//! The test FAILS today: the bridge releases `10^18` raw, which is
//! `10^12` whole tokens on a 6-decimal ERC-20.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use alloy::{
    network::EthereumWallet, primitives::U256, providers::ProviderBuilder,
    signers::local::PrivateKeySigner, sol,
};
use anyhow::Context as _;
use futures::StreamExt as _;
use linera_base::{
    crypto::InMemorySigner,
    data_types::{Amount, Bytecode},
    identifiers::AccountOwner,
    vm::VmRuntime,
};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token_with_decimals,
    fund_bridge_erc20, light_client_address, start_compose, wait_for_light_client,
    ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{environment::wallet::Memory, worker::Reason};
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use wrapped_fungible::{Account, InitialState, WrappedFungibleOperation, WrappedParameters};

const TOKEN_DECIMALS: u8 = 6;
const ONE_EVM_TOKEN_RAW: u128 = 1_000_000; // 10^6
                                           // Must exceed `Amount::from_tokens(1).value = 10^18` so the (buggy) raw
                                           // release attempt doesn't revert the EVM transfer before the assertion fires.
const INITIAL_SUPPLY_EVM_RAW: u128 = 2_000_000_000_000_000_000;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
    }
}

#[tokio::test]
#[ignore]
async fn decimal_mismatch_linera_to_evm() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-bridge-decimal-mismatch-burn-test";

    let compose = start_compose(&compose_file, project_name).await;
    wait_for_light_client(&compose, project_name, &compose_file).await;

    // ── Programmatic Linera client ──
    let faucet = Faucet::new("http://localhost:8080".to_string());
    let genesis_config = faucet.genesis_config().await?;

    let config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &config,
        "decimal-mismatch-burn-e2e-test",
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

    // Chain A: bridge / minter. Chain B: user.
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
    let mut notifications_b = cc_b.subscribe()?;
    let (listener_b, _abort_b, _) = cc_b.listen().await?;
    tokio::spawn(listener_b);

    let mut notifications_a = cc_a.subscribe()?;
    let (listener_a, _abort_a, _) = cc_a.listen().await?;
    tokio::spawn(listener_a);

    tracing::info!(%chain_a, %owner_a, %chain_b, %owner_b, "Chains claimed");

    // ── Deploy 6-decimal token on EVM ──
    let erc20_addr = deploy_linera_token_with_decimals(
        &compose,
        project_name,
        &compose_file,
        TOKEN_DECIMALS,
        INITIAL_SUPPLY_EVM_RAW,
    )
    .await?;
    tracing::info!(%erc20_addr, decimals = TOKEN_DECIMALS, "6-decimal token deployed");

    // ── Publish wrapped-fungible module ──
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .context("manifest dir has fewer than 3 ancestors")?
        .to_path_buf();
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");

    let wf_contract = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_contract.wasm"))?;
    let wf_service = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_service.wasm"))?;
    let (wf_module_id, _) = cc_a
        .publish_module(wf_contract, wf_service, VmRuntime::Wasm)
        .await?
        .expect("publish wrapped-fungible module committed");

    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;

    // ── Create wrapped-fungible app on chain A, seeding owner_a with 1 wTEST ──
    let wrapped_params = WrappedParameters {
        ticker_symbol: "wTEST".to_string(),
        decimals: TOKEN_DECIMALS,
        minter: Some(owner_a),
        mint_chain_id: Some(chain_a),
        evm_token_address: erc20_addr.0 .0,
        evm_source_chain_id: 31337,
        bridge_app_id: None,
    };
    let wrapped_init = InitialState {
        accounts: BTreeMap::from([(owner_a, Amount::from_tokens(1))]),
    };
    let (fungible_app_id, _) = cc_a
        .create_application_untyped(
            wf_module_id,
            serde_json::to_vec(&wrapped_params)?,
            serde_json::to_vec(&wrapped_init)?,
            vec![],
        )
        .await?
        .expect("create wrapped-fungible app committed");

    // ── Deploy FungibleBridge.sol ──
    let chain_a_bytes32 = format!("0x{chain_a}");
    let app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
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
    tracing::info!(%bridge_addr, "FungibleBridge deployed");

    // ── Fund bridge with enough raw to cover the release ──
    fund_bridge_erc20(
        &compose,
        project_name,
        &compose_file,
        erc20_addr,
        bridge_addr,
        INITIAL_SUPPLY_EVM_RAW,
    )
    .await;

    // ── owner_a transfers 1 wTEST cross-chain to (chain_b, owner_b) ──
    let transfer_a_to_b = WrappedFungibleOperation::Transfer {
        owner: owner_a,
        amount: Amount::from_tokens(1).to_attos().to_string(),
        target_account: Account {
            chain_id: chain_b,
            owner: owner_b,
        },
    };
    cc_a.execute_operations(
        vec![Operation::User {
            application_id: fungible_app_id,
            bytes: bcs::to_bytes(&transfer_a_to_b)?,
        }],
        vec![],
    )
    .await?
    .expect("transfer A→B committed");

    // ── Wait for Credit on chain B + process inbox ──
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(notification) = notifications_b.next().await {
            if matches!(notification.reason, Reason::NewIncomingBundle { .. }) {
                return;
            }
        }
        panic!("Notification stream ended without NewIncomingBundle on chain B");
    })
    .await?;
    cc_b.synchronize_from_validators().await?;
    cc_b.process_inbox().await?;

    // ── owner_b transfers 1 wTEST to (chain_a, Address20) → auto-burn on chain A ──
    let evm_recipient_hex = "70997970C51812dc3A010C7d01b50e0d17dc79C8";
    let evm_recipient_owner: AccountOwner = format!("0x{evm_recipient_hex}").parse()?;
    let burn_transfer = WrappedFungibleOperation::Transfer {
        owner: owner_b,
        amount: Amount::from_tokens(1).to_attos().to_string(),
        target_account: Account {
            chain_id: chain_a,
            owner: evm_recipient_owner,
        },
    };
    cc_b.execute_operations(
        vec![Operation::User {
            application_id: fungible_app_id,
            bytes: bcs::to_bytes(&burn_transfer)?,
        }],
        vec![],
    )
    .await?
    .expect("burn-transfer committed");

    // ── Wait for Credit on chain A + process inbox (triggers auto-burn) ──
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(notification) = notifications_a.next().await {
            if matches!(notification.reason, Reason::NewIncomingBundle { .. }) {
                return;
            }
        }
        panic!("Notification stream ended without NewIncomingBundle on chain A");
    })
    .await?;
    cc_a.synchronize_from_validators().await?;
    let (burn_certs, _) = cc_a.process_inbox().await?;
    assert!(
        !burn_certs.is_empty(),
        "process_inbox on chain A must produce a burn cert"
    );

    // ── Submit burn certs to FungibleBridge.addBlock ──
    let rpc_url = "http://localhost:8545".parse()?;
    let evm_signer: PrivateKeySigner = ANVIL_PRIVATE_KEY.parse()?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .connect_http(rpc_url);

    let bridge_contract = IFungibleBridge::new(bridge_addr, &provider);
    for cert in &burn_certs {
        bridge_contract
            .addBlock(bcs::to_bytes(cert)?.into())
            .send()
            .await?
            .get_receipt()
            .await?;
    }

    // ── Assert EVM recipient got exactly 1 whole token (= ONE_EVM_TOKEN_RAW raw) ──
    let evm_recipient_addr: alloy::primitives::Address =
        format!("0x{evm_recipient_hex}").parse()?;
    let erc20_contract = IERC20::new(erc20_addr, &provider);
    let received = erc20_contract.balanceOf(evm_recipient_addr).call().await?;
    let expected = U256::from(ONE_EVM_TOKEN_RAW);
    assert_eq!(
        received, expected,
        "EVM recipient must receive exactly 1 whole token (= {ONE_EVM_TOKEN_RAW} raw) \
         for a Linera-side burn of Amount::from_tokens(1); got {received} raw",
    );
    tracing::info!(%received, "Burn assertion passed");

    Ok(())
}

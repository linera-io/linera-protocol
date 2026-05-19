// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

//! End-to-end regression test for the EVM→Linera leg of the bridge against
//! a non-18-decimal ERC-20 (here: 6, like USDC).
//!
//! The bridge stores deposited EVM amounts as raw `u128` (no decimal
//! scaling). A deposit of `N` raw on the EVM side must end up as
//! `Amount(N)` on Linera — i.e. `linera_balance.to_attos() == N`.
//! The string representation of that `Amount` (using Linera's hardcoded
//! 18-decimal `Display`) will look like a near-zero value for low-decimal
//! tokens, but that's a UI concern, not a contract one.
//!
//! Flow:
//! 1. Deploy a 6-decimal ERC-20, the EVM `FungibleBridge`, and the matching
//!    Linera evm-bridge + wrapped-fungible apps.
//! 2. Approve + `deposit(100 * 10^6 raw)` on EVM.
//! 3. Generate the deposit proof and submit `ProcessDeposit` on Linera.
//! 4. Assert that `owner_b`'s wrapped balance on Linera, read as attos,
//!    equals `DEPOSIT_RAW`.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use alloy::{
    network::EthereumWallet,
    primitives::{FixedBytes, U256},
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
    sol,
};
use anyhow::Context as _;
use futures::StreamExt as _;
use linera_base::{
    crypto::InMemorySigner,
    data_types::{Amount, Bytecode},
    identifiers::AccountOwner,
    vm::VmRuntime,
};
use linera_bridge::{
    abi::{BridgeInstantiationArgument, BridgeOperation, BridgeParameters},
    proof::gen::{DepositProofClient as _, HttpDepositProofClient},
};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token_with_decimals,
    light_client_address, start_compose, wait_for_light_client, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{environment::wallet::Memory, worker::Reason};
use linera_execution::{Operation, Query, QueryResponse, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use serde::Serialize;
use wrapped_fungible::{InitialState, WrappedParameters};

const TOKEN_DECIMALS: u8 = 6;
const ONE_EVM_TOKEN_RAW: u128 = 1_000_000; // 10^6
const INITIAL_SUPPLY_EVM_RAW: u128 = 1_000_000 * ONE_EVM_TOKEN_RAW;
const DEPOSIT_RAW: u128 = 100 * ONE_EVM_TOKEN_RAW;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
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
#[ignore]
async fn decimal_mismatch_evm_to_linera() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-bridge-decimal-mismatch-evm-to-linera-test";

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
        "decimal-mismatch-evm-to-linera-e2e-test",
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

    // Chain A: bridge / minter. Chain B: user (deposit target).
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

    // ── Publish modules ──
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .context("manifest dir has fewer than 3 ancestors")?
        .to_path_buf();
    let evm_bridge_wasm_dir =
        repo_root.join("linera-bridge/contracts/evm-bridge/target/wasm32-unknown-unknown/release");
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");

    let wf_contract = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_contract.wasm"))?;
    let wf_service = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_service.wasm"))?;
    let (wf_module_id, _) = cc_a
        .publish_module(wf_contract, wf_service, VmRuntime::Wasm)
        .await?
        .expect("publish wrapped-fungible module committed");

    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;

    let eb_contract =
        Bytecode::load_from_file(evm_bridge_wasm_dir.join("evm_bridge_contract.wasm"))?;
    let eb_service = Bytecode::load_from_file(evm_bridge_wasm_dir.join("evm_bridge_service.wasm"))?;
    let (eb_module_id, _) = cc_a
        .publish_module(eb_contract, eb_service, VmRuntime::Wasm)
        .await?
        .expect("publish evm-bridge module committed");

    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;

    // ── Create evm-bridge app on chain A ──
    let bridge_params = BridgeParameters {
        source_chain_id: 31337,
        token_address: erc20_addr.0 .0,
    };
    let (bridge_app_id, _) = cc_a
        .create_application_untyped(
            eb_module_id,
            serde_json::to_vec(&bridge_params)?,
            serde_json::to_vec(&BridgeInstantiationArgument {
                rpc_endpoint: String::new(),
            })?,
            vec![],
        )
        .await?
        .expect("create evm-bridge app committed");

    // ── Create wrapped-fungible app on chain A ──
    let wrapped_params = WrappedParameters {
        ticker_symbol: "wTEST".to_string(),
        minter: Some(owner_a),
        mint_chain_id: Some(chain_a),
        evm_token_address: erc20_addr.0 .0,
        evm_source_chain_id: 31337,
        bridge_app_id: Some(bridge_app_id),
    };
    let wrapped_init = InitialState {
        accounts: BTreeMap::new(),
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

    // ── Register fungible app in bridge ──
    let register_op = BridgeOperation::RegisterFungibleApp {
        app_id: fungible_app_id,
    };
    cc_a.execute_operations(
        vec![Operation::User {
            application_id: bridge_app_id,
            bytes: bcs::to_bytes(&register_op)?,
        }],
        vec![],
    )
    .await?
    .expect("register fungible app committed");

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

    // ── Register FungibleBridge address in evm-bridge ──
    let register_bridge_op = BridgeOperation::RegisterFungibleBridge {
        address: bridge_addr.0 .0,
    };
    cc_a.execute_operations(
        vec![Operation::User {
            application_id: bridge_app_id,
            bytes: bcs::to_bytes(&register_bridge_op)?,
        }],
        vec![],
    )
    .await?
    .expect("register bridge contract address committed");

    // ── EVM deposit: 100 * 10^6 raw, targeting (chain_b, owner_b) ──
    let rpc_url = "http://localhost:8545".parse()?;
    let evm_signer: PrivateKeySigner = ANVIL_PRIVATE_KEY.parse()?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .connect_http(rpc_url);

    let deposit_amount_u256 = U256::from(DEPOSIT_RAW);
    let erc20_contract = IERC20::new(erc20_addr, &provider);
    erc20_contract
        .approve(bridge_addr, deposit_amount_u256)
        .send()
        .await?
        .get_receipt()
        .await?;

    let chain_b_b256: FixedBytes<32> = {
        let bytes: [u8; 32] = chain_b.0.into();
        FixedBytes::<32>::from(bytes)
    };
    let fungible_app_b256: FixedBytes<32> = app_id_bytes32.parse()?;
    let owner_b_b256 = match owner_b {
        AccountOwner::Address32(hash) => {
            let bytes: [u8; 32] = hash.into();
            FixedBytes::<32>::from(bytes)
        }
        _ => anyhow::bail!("expected Address32 owner_b"),
    };

    let bridge_contract = IFungibleBridge::new(bridge_addr, &provider);
    let deposit_receipt = bridge_contract
        .deposit(
            chain_b_b256,
            fungible_app_b256,
            owner_b_b256,
            deposit_amount_u256,
        )
        .send()
        .await?
        .get_receipt()
        .await?;
    let deposit_tx_hash = deposit_receipt.transaction_hash;
    tracing::info!(?deposit_tx_hash, raw = DEPOSIT_RAW, "EVM deposit confirmed");

    // ── Generate proof + ProcessDeposit on chain A ──
    let proof_client = HttpDepositProofClient::new("http://localhost:8545")?;
    let proof = proof_client.generate_deposit_proof(deposit_tx_hash).await?;
    let tx_index = proof.tx_index;
    let log_index = proof.log_indices[0];

    let process_op = BridgeOperation::ProcessDeposit {
        block_header_rlp: proof.block_header_rlp,
        receipt_rlp: proof.receipt_rlp,
        proof_nodes: proof.proof_nodes,
        tx_index,
        log_index,
    };
    cc_a.execute_operations(
        vec![Operation::User {
            application_id: bridge_app_id,
            bytes: bcs::to_bytes(&process_op)?,
        }],
        vec![],
    )
    .await?
    .expect("ProcessDeposit committed");
    tracing::info!("ProcessDeposit executed on chain A");

    // ── Wait for Credit message on chain B, then process inbox ──
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(notification) = notifications_b.next().await {
            if matches!(notification.reason, Reason::NewIncomingBundle { .. }) {
                return;
            }
        }
        panic!("Notification stream ended without NewIncomingBundle");
    })
    .await?;
    cc_b.synchronize_from_validators().await?;
    cc_b.process_inbox().await?;

    // ── Assert wrapped balance on chain B equals 100 whole wTEST ──
    let gql_query = format!(
        r#"query {{ accounts {{ entry(key: "{}") {{ value }} }} }}"#,
        owner_b
    );
    #[derive(Serialize)]
    struct GqlRequest {
        query: String,
    }
    let query = Query::user_without_abi(fungible_app_id, &GqlRequest { query: gql_query })?;
    let (outcome, _) = cc_b.query_application(query, None).await?;
    let response_bytes = match outcome.response {
        QueryResponse::User(bytes) => bytes,
        other => anyhow::bail!("unexpected query response: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_slice(&response_bytes)?;
    let balance_str = response["data"]["accounts"]["entry"]["value"]
        .as_str()
        .context("no balance in GraphQL response")?;
    let linera_balance: Amount = balance_str.parse()?;

    assert_eq!(
        linera_balance.to_attos(),
        DEPOSIT_RAW,
        "Linera balance after deposit must preserve the deposited raw value: \
         deposited {DEPOSIT_RAW} raw of a 6-decimal token, \
         got {} attos on Linera",
        linera_balance.to_attos(),
    );
    tracing::info!(%linera_balance, "Deposit assertion passed");

    Ok(())
}

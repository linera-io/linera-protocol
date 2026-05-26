// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

//! End-to-end test: deposit ERC-20 tokens on EVM (Anvil), generate an MPT proof,
//! submit a `ProcessDeposit` operation to the evm-bridge app on Linera, and verify
//! that the wrapped-fungible tokens are minted.

use std::{collections::BTreeMap, path::PathBuf};

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
    data_types::{Bytecode, U128},
    identifiers::AccountOwner,
    vm::VmRuntime,
};
use linera_bridge::{
    abi::{BridgeInstantiationArgument, BridgeOperation, BridgeParameters},
    proof::gen::{DepositProofClient as _, HttpDepositProofClient},
};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token, light_client_address,
    start_compose, wait_for_light_client, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, Query, QueryResponse, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use serde::Serialize;
use wrapped_fungible::{InitialState, WrappedParameters};

// ── Solidity interfaces for EVM calls ───────────────────────────────────────

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
#[ignore] // Requires pre-built docker images and Wasm: `make -C linera-bridge build-all`
async fn test_evm_to_linera_bridge() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-e2l-bridge-test";

    // ── Phase 1: Start docker compose stack ──
    let compose = start_compose(&compose_file, project_name).await;
    wait_for_light_client(&compose, project_name, &compose_file).await;

    // ── Phase 2: Create programmatic Linera client and claim chain ──
    tracing::info!("Creating programmatic Linera client...");
    let faucet = Faucet::new("http://localhost:8080".to_string());
    let genesis_config = faucet.genesis_config().await?;

    let config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &config,
        "e2l-bridge-e2e-test",
        Some(WasmRuntime::default()),
        StorageCacheConfig {
            blob_cache_size: 1000,
            confirmed_block_cache_size: 1000,
            certificate_cache_size: 1000,
            certificate_raw_cache_size: 1000,
            event_cache_size: 1000,
            block_hash_by_height_cache_size: 1000,
            event_block_height_cache_size: 1000,
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

    tracing::info!("Claiming chain from faucet...");
    let owner = AccountOwner::from(signer.generate_new());
    let chain_desc = faucet.claim(&owner).await?;
    let chain_id = chain_desc.id();
    ctx.extend_with_chain(chain_desc, Some(owner)).await?;

    let cc = ctx.make_chain_client(chain_id).await?;
    cc.synchronize_from_validators().await?;
    tracing::info!(%chain_id, %owner, "Chain claimed");

    // ── Phase 3: Deploy LineraToken on Anvil ──
    tracing::info!("Deploying LineraToken via forge script...");
    let erc20_addr = deploy_linera_token(&compose, project_name, &compose_file).await?;
    tracing::info!(%erc20_addr, "LineraToken deployed");

    let chain_id_bytes32 = format!("0x{chain_id}");

    // ── Phase 4: Deploy wrapped-fungible + evm-bridge apps on Linera ──
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .context("manifest dir has fewer than 3 ancestors")?
        .to_path_buf();
    let evm_bridge_wasm_dir =
        repo_root.join("linera-bridge/contracts/evm-bridge/target/wasm32-unknown-unknown/release");
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");

    // 4a. Publish and create wrapped-fungible app
    tracing::info!("Publishing wrapped-fungible module...");
    let wf_contract = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_contract.wasm")).await?;
    let wf_service = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_service.wasm")).await?;

    let (wf_module_id, _) = cc
        .publish_module(wf_contract, wf_service, VmRuntime::Wasm, None)
        .await?
        .expect("publish wrapped-fungible module committed");

    cc.synchronize_from_validators().await?;
    cc.process_inbox().await?;

    // 4b. Publish evm-bridge module (created after wrapped-fungible so it can
    // take the wrapped applicationId as a creation parameter).
    tracing::info!("Publishing evm-bridge module...");
    let eb_contract =
        Bytecode::load_from_file(evm_bridge_wasm_dir.join("evm_bridge_contract.wasm")).await?;
    let eb_service =
        Bytecode::load_from_file(evm_bridge_wasm_dir.join("evm_bridge_service.wasm")).await?;

    let (eb_module_id, _) = cc
        .publish_module(eb_contract, eb_service, VmRuntime::Wasm, None)
        .await?
        .expect("publish evm-bridge module committed");

    cc.synchronize_from_validators().await?;
    cc.process_inbox().await?;

    // 4c. Create wrapped-fungible app first (its id is a parameter of the bridge)
    tracing::info!("Creating wrapped-fungible application...");
    let wrapped_params = WrappedParameters {
        ticker_symbol: "wTEST".to_string(),
        decimals: 18,
        mint_chain_id: chain_id,
        evm_token_address: erc20_addr.0 .0,
        evm_source_chain_id: 31337,
    };
    let wrapped_init = InitialState {
        accounts: BTreeMap::new(),
    };
    let (fungible_app_id, _) = cc
        .create_application_untyped(
            wf_module_id,
            serde_json::to_vec(&wrapped_params)?,
            serde_json::to_vec(&wrapped_init)?,
            vec![],
        )
        .await?
        .expect("create wrapped-fungible app committed");
    tracing::info!(%fungible_app_id, "wrapped-fungible app created");

    // 4d. Create evm-bridge app with the wrapped-fungible app id as a parameter
    tracing::info!("Creating evm-bridge application...");
    let bridge_params = BridgeParameters {
        source_chain_id: 31337,
        token_address: erc20_addr.0 .0,
        bridge_chain_id: chain_id,
        fungible_app_id,
    };
    let (bridge_app_id, _) = cc
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
    tracing::info!(%bridge_app_id, "evm-bridge app created");

    // 4e. Register the bridge app on the wrapped-fungible app
    tracing::info!("Registering bridge app in wrapped-fungible...");
    let register_op = wrapped_fungible::WrappedFungibleOperation::RegisterAuthorizedCaller {
        app_id: bridge_app_id,
    };
    let register_bytes = bcs::to_bytes(&register_op)?;
    let register_operation = Operation::User {
        application_id: fungible_app_id,
        bytes: register_bytes,
    };
    cc.execute_operations(vec![register_operation], vec![])
        .await?
        .expect("register bridge app committed");

    // 4f. Deploy FungibleBridge with the wrapped-fungible applicationId baked in
    let app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
    let bridge_app_id_bytes32 = format!("0x{}", bridge_app_id.application_description_hash);
    let light_client = light_client_address();
    tracing::info!("Deploying FungibleBridge via forge script...");
    let bridge_addr = deploy_fungible_bridge(
        &compose,
        project_name,
        &compose_file,
        light_client,
        &chain_id_bytes32,
        erc20_addr,
        &app_id_bytes32,
        &bridge_app_id_bytes32,
    )
    .await?;
    tracing::info!(%bridge_addr, "FungibleBridge deployed");

    // 4g. Register the FungibleBridge contract address in the evm-bridge app
    tracing::info!("Registering FungibleBridge address in evm-bridge...");
    let register_bridge_op = BridgeOperation::RegisterFungibleBridge {
        address: bridge_addr.0 .0,
    };
    let register_bridge_bytes = bcs::to_bytes(&register_bridge_op)?;
    let register_bridge_operation = Operation::User {
        application_id: bridge_app_id,
        bytes: register_bridge_bytes,
    };
    cc.execute_operations(vec![register_bridge_operation], vec![])
        .await?
        .expect("register bridge contract address committed");

    // ── Phase 5: Approve + Deposit on EVM ──
    let deposit_amount = U256::from(100u128 * 10u128.pow(18)); // 100 tokens

    tracing::info!("Setting up alloy provider...");
    let rpc_url = "http://localhost:8545".parse()?;
    let evm_signer: PrivateKeySigner = ANVIL_PRIVATE_KEY.parse()?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .connect_http(rpc_url);

    tracing::info!("Approving FungibleBridge to spend ERC20 tokens...");
    let erc20_contract = IERC20::new(erc20_addr, &provider);
    let approve_tx = erc20_contract
        .approve(bridge_addr, deposit_amount)
        .send()
        .await?;
    let approve_receipt = approve_tx.get_receipt().await?;
    tracing::info!(tx=?approve_receipt.transaction_hash, "Approve confirmed");

    let chain_b256 = {
        let bytes: [u8; 32] = chain_id.0.into();
        FixedBytes::<32>::from(bytes)
    };
    let fungible_app_b256: FixedBytes<32> = app_id_bytes32.parse()?;
    let owner_b256 = match owner {
        AccountOwner::Address32(hash) => {
            let bytes: [u8; 32] = hash.into();
            FixedBytes::<32>::from(bytes)
        }
        _ => anyhow::bail!("expected Address32 owner"),
    };

    tracing::info!("Calling deposit on FungibleBridge...");
    let bridge_contract = IFungibleBridge::new(bridge_addr, &provider);
    let deposit_tx = bridge_contract
        .deposit(chain_b256, fungible_app_b256, owner_b256, deposit_amount)
        .send()
        .await?;
    let deposit_receipt = deposit_tx.get_receipt().await?;
    let deposit_tx_hash = deposit_receipt.transaction_hash;
    tracing::info!(?deposit_tx_hash, "Deposit confirmed");

    // ── Phase 6: Generate deposit proof ──
    tracing::info!("Generating deposit proof...");
    let proof_client = HttpDepositProofClient::new("http://localhost:8545")?;
    let proof = proof_client.generate_deposit_proof(deposit_tx_hash).await?;
    tracing::info!(
        tx_index = proof.tx_index,
        log_indices = ?proof.log_indices,
        proof_nodes = proof.proof_nodes.len(),
        "Deposit proof generated"
    );

    // Build the DepositKey for completion checks.
    let tx_index = proof.tx_index;
    let log_index = proof.log_indices[0];
    let deposit_key = linera_bridge::proof::DepositKey {
        source_chain_id: 31337, // Anvil chain ID
        block_hash: deposit_receipt.block_hash.unwrap(),
        tx_index,
        log_index,
    };

    // ── Phase 7a: Verify deposit is NOT yet processed ──
    assert!(
        !linera_bridge_e2e::query_deposit_processed(&cc, bridge_app_id, &deposit_key).await?,
        "deposit should NOT be processed before ProcessDeposit"
    );
    tracing::info!("Confirmed: deposit not yet processed.");

    // ── Phase 7b: Submit ProcessDeposit on Linera ──
    tracing::info!("Submitting ProcessDeposit operation...");
    let bridge_op = BridgeOperation::ProcessDeposit {
        block_header_rlp: proof.block_header_rlp,
        receipt_rlp: proof.receipt_rlp,
        proof_nodes: proof.proof_nodes,
        tx_index,
        log_index,
    };
    let op_bytes = bcs::to_bytes(&bridge_op)?;
    let op = Operation::User {
        application_id: bridge_app_id,
        bytes: op_bytes,
    };

    let cert = cc
        .execute_operations(vec![op], vec![])
        .await?
        .expect("ProcessDeposit committed");
    tracing::info!(
        height = ?cert.inner().block().header.height,
        "ProcessDeposit executed successfully"
    );

    // ── Phase 8: Verify wrapped-fungible balance ──
    tracing::info!("Querying wrapped-fungible balance...");
    let gql_query = format!(
        r#"query {{ accounts {{ entry(key: "{}") {{ value }} }} }}"#,
        owner
    );

    #[derive(Serialize)]
    struct GqlRequest {
        query: String,
    }

    let query = Query::user_without_abi(fungible_app_id, &GqlRequest { query: gql_query })?;
    let (outcome, _) = cc.query_application(query, None).await?;
    let response_bytes = match outcome.response {
        QueryResponse::User(bytes) => bytes,
        other => anyhow::bail!("unexpected query response: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_slice(&response_bytes)?;
    tracing::info!(?response, "GraphQL response");

    let balance_str = response["data"]["accounts"]["entry"]["value"]
        .as_str()
        .context("no balance in GraphQL response")?;
    let balance: U128 = balance_str.parse()?;
    assert_eq!(
        balance,
        U128(100u128 * 10u128.pow(18)),
        "wrapped-fungible balance should match the 100-token deposit"
    );

    tracing::info!(%balance, "Wrapped-fungible balance matches deposit.");

    // ── Phase 9: Verify deposit IS now processed ──
    assert!(
        linera_bridge_e2e::query_deposit_processed(&cc, bridge_app_id, &deposit_key).await?,
        "deposit should be marked as processed after ProcessDeposit"
    );
    tracing::info!("Test passed! Deposit confirmed as processed via GraphQL query.");
    Ok(())
}

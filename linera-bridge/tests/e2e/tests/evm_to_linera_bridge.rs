// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

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
    data_types::{Amount, Bytecode},
    identifiers::{AccountOwner, ApplicationId},
    vm::VmRuntime,
};
use linera_bridge::proof::gen::{DepositProofClient as _, HttpDepositProofClient};
use linera_bridge_e2e::{
    compose_file_path, exec_output, light_client_address, parse_deployed_address, start_compose,
    ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, Query, QueryResponse, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::DbStorage;
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use serde::{Deserialize, Serialize};
use wrapped_fungible::{InitialState, WrappedParameters};

// ── Inline evm-bridge types (avoids depending on evm-bridge crate) ──────────

/// Must match `evm_bridge::BridgeParameters` field-for-field for BCS compatibility.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct BridgeParameters {
    source_chain_id: u64,
    bridge_contract_address: [u8; 20],
    fungible_app_id: ApplicationId,
    token_address: [u8; 20],
}

/// Must match `evm_bridge::BridgeOperation` variant-for-variant for BCS compatibility.
#[derive(Debug, Deserialize, Serialize)]
enum BridgeOperation {
    ProcessDeposit {
        block_header_rlp: Vec<u8>,
        receipt_rlp: Vec<u8>,
        proof_nodes: Vec<Vec<u8>>,
        tx_index: u64,
        log_index: u64,
    },
}

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
    let compose_file = compose_file_path();
    let project_name = "linera-e2l-bridge-test";

    // ── Phase 1: Start docker compose stack ──
    let compose = start_compose(&compose_file, project_name).await;

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

    // ── Phase 3: Deploy MockERC20 + FungibleBridge on Anvil ──
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

    let chain_id_bytes32 = format!("0x{chain_id}");
    let zero_bytes32 =
        "0x0000000000000000000000000000000000000000000000000000000000000000";

    tracing::info!("Deploying FungibleBridge (applicationId = 0x0)...");
    let light_client = light_client_address();
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
             {chain_id_bytes32} \
             0 \
             {zero_bytes32} \
             {erc20_addr}"
        ),
        project_name,
        &compose_file,
    )
    .await;
    let bridge_addr = parse_deployed_address(&bridge_output)?;
    tracing::info!(%bridge_addr, "FungibleBridge deployed");

    // ── Phase 4: Deploy wrapped-fungible + evm-bridge apps on Linera ──
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .context("manifest dir has fewer than 3 ancestors")?
        .to_path_buf();
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");

    // 4a. Publish and create wrapped-fungible app
    tracing::info!("Publishing wrapped-fungible module...");
    let wf_contract = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_contract.wasm"))?;
    let wf_service = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_service.wasm"))?;

    let (wf_module_id, _) = cc
        .publish_module(wf_contract, wf_service, VmRuntime::Wasm)
        .await?
        .expect("publish wrapped-fungible module committed");

    cc.synchronize_from_validators().await?;
    cc.process_inbox().await?;

    tracing::info!("Creating wrapped-fungible application...");
    let wrapped_params = WrappedParameters {
        ticker_symbol: "wTEST".to_string(),
        minter: owner,
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

    // 4b. Publish and create evm-bridge app
    tracing::info!("Publishing evm-bridge module...");
    let eb_contract = Bytecode::load_from_file(wasm_dir.join("evm_bridge_contract.wasm"))?;
    let eb_service = Bytecode::load_from_file(wasm_dir.join("evm_bridge_service.wasm"))?;

    let (eb_module_id, _) = cc
        .publish_module(eb_contract, eb_service, VmRuntime::Wasm)
        .await?
        .expect("publish evm-bridge module committed");

    cc.synchronize_from_validators().await?;
    cc.process_inbox().await?;

    tracing::info!("Creating evm-bridge application...");
    let bridge_params = BridgeParameters {
        source_chain_id: 31337,
        bridge_contract_address: bridge_addr.0 .0,
        fungible_app_id,
        token_address: erc20_addr.0 .0,
    };
    let (bridge_app_id, _) = cc
        .create_application_untyped(
            eb_module_id,
            serde_json::to_vec(&bridge_params)?,
            serde_json::to_vec(&())?,
            vec![fungible_app_id],
        )
        .await?
        .expect("create evm-bridge app committed");
    tracing::info!(%bridge_app_id, "evm-bridge app created");

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
    let zero_b256 = FixedBytes::<32>::ZERO;
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
        .deposit(chain_b256, zero_b256, owner_b256, deposit_amount)
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
        log_index = proof.log_index,
        proof_nodes = proof.proof_nodes.len(),
        "Deposit proof generated"
    );

    // ── Phase 7: Submit ProcessDeposit on Linera ──
    tracing::info!("Submitting ProcessDeposit operation...");
    let bridge_op = BridgeOperation::ProcessDeposit {
        block_header_rlp: proof.block_header_rlp,
        receipt_rlp: proof.receipt_rlp,
        proof_nodes: proof.proof_nodes,
        tx_index: proof.tx_index,
        log_index: proof.log_index,
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
    let outcome = cc.query_application(query, None).await?;
    let response_bytes = match outcome.response {
        QueryResponse::User(bytes) => bytes,
        other => anyhow::bail!("unexpected query response: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_slice(&response_bytes)?;
    tracing::info!(?response, "GraphQL response");

    let balance_str = response["data"]["accounts"]["entry"]["value"]
        .as_str()
        .context("no balance in GraphQL response")?;
    let balance: Amount = balance_str.parse()?;
    assert_eq!(
        balance,
        Amount::from_tokens(100),
        "wrapped-fungible balance should match the 100-token deposit"
    );

    tracing::info!(%balance, "Test passed! Wrapped-fungible balance matches deposit.");
    Ok(())
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end test: deploy a fungible token on Linera, transfer tokens to an EVM address,
//! submit the block certificate to FungibleBridge on Anvil, and verify the ERC20 balance.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use alloy::{
    network::EthereumWallet,
    primitives::{Address, U256},
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
use linera_bridge_e2e::{
    compose_file_path, exec_ok, exec_output, light_client_address, start_compose, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{environment::wallet::Memory, worker::Reason};
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_sdk::abis::fungible::{self, FungibleOperation, FungibleTokenAbi};
use linera_storage::DbStorage;
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
    }
}

/// Parse a "Deployed to: 0x..." address from forge create output.
fn parse_deployed_address(output: &str) -> anyhow::Result<Address> {
    for line in output.lines() {
        if let Some(addr) = line.strip_prefix("Deployed to: ") {
            return Ok(addr.trim().parse()?);
        }
    }
    anyhow::bail!("Could not find 'Deployed to:' in forge output:\n{output}");
}

#[tokio::test]
#[ignore] // Requires pre-built docker images and Wasm: `make -C linera-bridge build-all`
async fn test_fungible_bridge_transfers_to_evm() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    let compose_file = compose_file_path();
    let project_name = "linera-bridge-test";

    let compose = start_compose(&compose_file, project_name).await;

    // ── 1. Create programmatic Linera client ──
    tracing::info!("Creating programmatic Linera client...");
    let faucet = Faucet::new("http://localhost:8080".to_string());
    let genesis_config = faucet.genesis_config().await?;

    let config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &config,
        "bridge-e2e-test",
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

    // ── 2. Claim chain A from faucet ──
    tracing::info!("Claiming chain A from faucet...");
    let owner_a = AccountOwner::from(signer.generate_new());
    let chain_a_desc = faucet.claim(&owner_a).await?;
    let chain_a = chain_a_desc.id();
    ctx.extend_with_chain(chain_a_desc, Some(owner_a)).await?;

    let cc_a = ctx.make_chain_client(chain_a).await?;
    cc_a.synchronize_from_validators().await?;
    tracing::info!(%chain_a, %owner_a, "Chain A claimed");

    // ── 3. Publish and create fungible app on chain A ──
    tracing::info!("Publishing fungible module...");
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("no parent dir")?
        .parent()
        .context("no grandparent dir")?
        .parent()
        .context("no great-grandparent dir")?
        .to_path_buf();
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");
    let contract_bytecode = Bytecode::load_from_file(wasm_dir.join("fungible_contract.wasm"))?;
    let service_bytecode = Bytecode::load_from_file(wasm_dir.join("fungible_service.wasm"))?;

    let (module_id, cert) = cc_a
        .publish_module(contract_bytecode, service_bytecode, VmRuntime::Wasm)
        .await?
        .expect("publish module committed");
    tracing::info!(height=?cert.inner().block().header.height, "Module published");

    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;

    tracing::info!("Creating fungible application...");
    let params = fungible::Parameters::new("TEST");
    let init_state = fungible::InitialState {
        accounts: BTreeMap::from([(owner_a, Amount::from_tokens(1000))]),
    };
    let (app_id, _cert): (linera_base::identifiers::ApplicationId<FungibleTokenAbi>, _) = cc_a
        .create_application(
            module_id.with_abi::<FungibleTokenAbi, _, _>(),
            &params,
            &init_state,
            vec![],
        )
        .await?
        .expect("create application committed");
    let app_id = app_id.forget_abi();
    tracing::info!(%app_id, "Application created");

    // ── 4. Claim chain B (bridge chain) from faucet and subscribe to notifications ──
    tracing::info!("Claiming chain B from faucet...");
    let owner_b = AccountOwner::from(signer.generate_new());
    let chain_b_desc = faucet.claim(&owner_b).await?;
    let chain_b = chain_b_desc.id();
    ctx.extend_with_chain(chain_b_desc, Some(owner_b)).await?;
    tracing::info!(%chain_b, "Chain B claimed");

    let cc_b = ctx.make_chain_client(chain_b).await?;
    let mut notifications = cc_b.subscribe()?;
    let (listener, _abort_handle, _) = cc_b.listen().await?;
    tokio::spawn(listener);

    // ── 5. Deploy MockERC20 on Anvil ──
    tracing::info!("Deploying MockERC20...");
    let erc20_output = exec_output(
        &compose,
        "foundry-tools",
        &format!(
            "forge create /contracts/MockERC20.sol:MockERC20 \
             --root /contracts --via-ir --optimize \
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

    // ── 6. Deploy FungibleBridge on Anvil ──
    let app_id_bytes32 = format!("0x{}", app_id.application_description_hash);
    let chain_b_bytes32 = format!("0x{chain_b}");

    tracing::info!("Deploying FungibleBridge...");
    let light_client = light_client_address();
    let bridge_output = exec_output(
        &compose,
        "foundry-tools",
        &format!(
            "forge create /contracts/FungibleBridge.sol:FungibleBridge \
             --root /contracts --via-ir --optimize \
             --ignored-error-codes 6321 \
             --out /tmp/forge-out --cache-path /tmp/forge-cache \
             --rpc-url http://anvil:8545 \
             --private-key {ANVIL_PRIVATE_KEY} \
             --broadcast \
             --constructor-args \
             {light_client} \
             {chain_b_bytes32} \
             0 \
             {app_id_bytes32} \
             {erc20_addr}"
        ),
        project_name,
        &compose_file,
    )
    .await;
    let bridge_addr = parse_deployed_address(&bridge_output)?;
    tracing::info!(%bridge_addr, "FungibleBridge deployed");

    // ── 7. Fund FungibleBridge with ERC20 tokens ──
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

    // ── 8. Transfer tokens from chain A to Address20 on chain B ──
    let evm_recipient = "70997970C51812dc3A010C7d01b50e0d17dc79C8";
    let receiver: AccountOwner = format!("0x{evm_recipient}").parse()?;

    tracing::info!("Sending fungible transfer to Address20 on chain B...");
    let transfer_bytes = bcs::to_bytes(&FungibleOperation::Transfer {
        owner: owner_a,
        amount: Amount::from_tokens(100),
        target_account: fungible::Account {
            chain_id: chain_b,
            owner: receiver,
        },
    })?;
    let transfer_op = Operation::User {
        application_id: app_id,
        bytes: transfer_bytes,
    };
    let transfer_cert = cc_a
        .execute_operations(vec![transfer_op], vec![])
        .await?
        .expect("transfer committed");
    let transfer_block = transfer_cert.inner().block();
    tracing::info!(
        height=?transfer_block.header.height,
        messages=transfer_block.body.messages.len(),
        recipients=?transfer_block.recipients(),
        "Transfer block submitted"
    );

    // ── 9. Wait for incoming bundle notification, then process inbox on chain B ──
    tracing::info!("Waiting for NewIncomingBundle notification on chain B...");
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(notification) = notifications.next().await {
            if matches!(notification.reason, Reason::NewIncomingBundle { .. }) {
                tracing::info!("Received NewIncomingBundle notification");
                return;
            }
        }
        panic!("Notification stream ended without NewIncomingBundle");
    })
    .await?;

    tracing::info!("Processing inbox on chain B...");
    cc_b.synchronize_from_validators().await?;
    let (certs, _) = cc_b.process_inbox().await?;
    tracing::info!(count = certs.len(), "Processed certificates from inbox");
    assert!(
        !certs.is_empty(),
        "process_inbox should produce at least one certificate"
    );

    // ── 10. Get certificate bytes for addBlock ──
    let cert = certs.last().context("no certificates from inbox")?;
    let cert_bytes = bcs::to_bytes(cert)?;
    tracing::info!(size = cert_bytes.len(), "Certificate serialized");

    // ── 11. Call addBlock() on FungibleBridge ──
    tracing::info!("Calling addBlock on FungibleBridge...");
    let rpc_url = "http://localhost:8545".parse()?;
    let evm_signer: PrivateKeySigner = ANVIL_PRIVATE_KEY.parse()?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .connect_http(rpc_url);

    let bridge_contract = IFungibleBridge::new(bridge_addr, &provider);
    let tx = bridge_contract.addBlock(cert_bytes.into()).send().await?;
    let receipt = tx.get_receipt().await?;
    tracing::info!(tx=?receipt.transaction_hash, "addBlock transaction submitted");

    // ── 12. Verify ERC20 balance ──
    let evm_recipient_addr: Address = format!("0x{evm_recipient}").parse()?;
    let erc20_contract = IERC20::new(erc20_addr, &provider);
    let balance = erc20_contract.balanceOf(evm_recipient_addr).call().await?;
    tracing::info!(?balance, "ERC20 balance of recipient");

    // 100 tokens = 100 * 10^18 (Amount uses 18 decimal places)
    let expected_balance = U256::from(100u128 * 10u128.pow(18));
    assert_eq!(
        balance, expected_balance,
        "ERC20 balance should match the transferred amount"
    );

    tracing::info!("Test passed! ERC20 balance matches transferred amount.");
    Ok(())
}
